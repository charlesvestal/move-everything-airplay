#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "plugin_api_v1.h"

#define RING_SECONDS        5
#define RING_SAMPLES        (MOVE_SAMPLE_RATE * 2 * RING_SECONDS)
#define AUDIO_IDLE_MS       3000
#define DEVICE_NAME_MAX     128
#define LOG_PATH            "/data/UserData/move-anything/cache/airplay-runtime.log"

static const host_api_v1_t *g_host = NULL;
static int g_instance_counter = 0;

typedef struct {
    char module_dir[512];
    char fifo_path[512];
    char config_path[512];
    char device_name[DEVICE_NAME_MAX];
    char error_msg[256];
    int slot;

    int fifo_fd;
    pid_t daemon_pid;
    bool daemon_running;

    int16_t ring[RING_SAMPLES];
    size_t write_pos;
    uint64_t write_abs;
    uint64_t play_abs;
    uint8_t pending_bytes[4];
    uint8_t pending_len;

    float gain;
    bool rec_source_mode;
    float peak_level;
    bool receiving_audio;
    bool paused;
    uint64_t last_audio_ms;
} airplay_instance_t;

static void append_log(const char *msg) {
    FILE *fp;
    if (!msg || msg[0] == '\0') return;
    fp = fopen(LOG_PATH, "a");
    if (!fp) return;
    fprintf(fp, "%s\n", msg);
    fclose(fp);
}

static void ap_log(const char *msg) {
    append_log(msg);
    if (g_host && g_host->log) {
        char buf[384];
        snprintf(buf, sizeof(buf), "[airplay] %s", msg);
        g_host->log(buf);
    }
}

static uint64_t now_ms(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (uint64_t)tv.tv_sec * 1000ULL + (uint64_t)tv.tv_usec / 1000ULL;
}

static void set_error(airplay_instance_t *inst, const char *msg) {
    if (!inst) return;
    snprintf(inst->error_msg, sizeof(inst->error_msg), "%s", msg ? msg : "unknown error");
    ap_log(inst->error_msg);
}

static void clear_error(airplay_instance_t *inst) {
    if (!inst) return;
    inst->error_msg[0] = '\0';
}

/* --- Ring buffer --- */

static size_t ring_available(const airplay_instance_t *inst) {
    uint64_t avail;
    if (!inst) return 0;
    if (inst->write_abs <= inst->play_abs) return 0;
    avail = inst->write_abs - inst->play_abs;
    if (avail > (uint64_t)RING_SAMPLES) avail = (uint64_t)RING_SAMPLES;
    return (size_t)avail;
}

static void ring_push(airplay_instance_t *inst, const int16_t *samples, size_t n) {
    size_t i;
    uint64_t oldest;
    for (i = 0; i < n; i++) {
        inst->ring[inst->write_pos] = samples[i];
        inst->write_pos = (inst->write_pos + 1) % RING_SAMPLES;
        inst->write_abs++;
    }

    oldest = 0;
    if (inst->write_abs > (uint64_t)RING_SAMPLES) {
        oldest = inst->write_abs - (uint64_t)RING_SAMPLES;
    }
    if (inst->play_abs < oldest) {
        inst->play_abs = oldest;
    }
}

static size_t ring_pop(airplay_instance_t *inst, int16_t *out, size_t n) {
    size_t got;
    size_t i;
    uint64_t abs_pos;

    if (!inst || !out || n == 0) return 0;

    got = ring_available(inst);
    if (got > n) got = n;
    abs_pos = inst->play_abs;

    for (i = 0; i < got; i++) {
        out[i] = inst->ring[(size_t)(abs_pos % (uint64_t)RING_SAMPLES)];
        abs_pos++;
    }

    inst->play_abs = abs_pos;
    return got;
}

static void clear_ring(airplay_instance_t *inst) {
    if (!inst) return;
    inst->write_pos = 0;
    inst->write_abs = 0;
    inst->play_abs = 0;
    inst->pending_len = 0;
    memset(inst->pending_bytes, 0, sizeof(inst->pending_bytes));
}

/* --- shairport-sync daemon management --- */

static int write_config(airplay_instance_t *inst) {
    FILE *fp;
    if (!inst) return -1;

    fp = fopen(inst->config_path, "w");
    if (!fp) {
        set_error(inst, "failed to write shairport-sync config");
        return -1;
    }

    fprintf(fp,
        "general = {\n"
        "  name = \"%s\";\n"
        "  port = %d;\n"
        "  udp_port_base = %d;\n"
        "  interpolation = \"basic\";\n"
        "  drift_tolerance_in_seconds = 0.002;\n"
        "};\n"
        "\n"
        "pipe = {\n"
        "  name = \"%s\";\n"
        "  audio_backend_buffer_desired_length_in_seconds = 0.2;\n"
        "};\n",
        inst->device_name,
        5000 + inst->slot - 1,    /* RTSP port: 5000, 5001, ... */
        6001 + (inst->slot - 1) * 10,  /* UDP base: 6001, 6011, ... */
        inst->fifo_path);

    fclose(fp);
    return 0;
}

static void stop_daemon(airplay_instance_t *inst) {
    int status;
    pid_t rc;

    if (!inst) return;

    if (inst->daemon_pid > 0) {
        rc = waitpid(inst->daemon_pid, &status, WNOHANG);
        if (rc == 0) {
            (void)kill(inst->daemon_pid, SIGTERM);
            usleep(300000);
            rc = waitpid(inst->daemon_pid, &status, WNOHANG);
            if (rc == 0) {
                (void)kill(inst->daemon_pid, SIGKILL);
                (void)waitpid(inst->daemon_pid, &status, 0);
            }
        }
        inst->daemon_pid = -1;
    }

    inst->daemon_running = false;
}

static int start_daemon(airplay_instance_t *inst) {
    pid_t pid;
    char log_msg[384];
    char shairport_path[1024];

    if (!inst) return -1;

    stop_daemon(inst);

    if (write_config(inst) != 0) {
        return -1;
    }

    snprintf(shairport_path, sizeof(shairport_path),
             "%s/bin/shairport-sync", inst->module_dir);

    pid = fork();
    if (pid < 0) {
        set_error(inst, "fork failed for shairport-sync");
        return -1;
    }

    if (pid == 0) {
        /* Child: redirect stderr to log, stdout to /dev/null */
        char lib_path[1024];
        int devnull = open("/dev/null", O_WRONLY);
        if (devnull >= 0) {
            dup2(devnull, STDOUT_FILENO);
            close(devnull);
        }
        int logfd = open(LOG_PATH, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (logfd >= 0) {
            dup2(logfd, STDERR_FILENO);
            close(logfd);
        }

        /* Add bundled libraries to search path */
        snprintf(lib_path, sizeof(lib_path), "%s/lib", inst->module_dir);
        setenv("LD_LIBRARY_PATH", lib_path, 1);

        /* Clear LD_PRELOAD so the Move shim hooks (ioctl, sendto, connect,
         * send, open, close, read) don't interfere with shairport-sync.
         * The D-Bus connect/send hooks in particular can intercept Avahi
         * communication and break AirPlay service registration. */
        unsetenv("LD_PRELOAD");

        execl(shairport_path, "shairport-sync",
              "-c", inst->config_path,
              "-o", "pipe",
              "-v",
              (char *)NULL);
        _exit(127);
    }

    inst->daemon_pid = pid;
    inst->daemon_running = true;
    clear_error(inst);

    snprintf(log_msg, sizeof(log_msg),
             "shairport-sync started pid=%d name=%s fifo=%s",
             (int)pid, inst->device_name, inst->fifo_path);
    ap_log(log_msg);

    return 0;
}

static void check_daemon_alive(airplay_instance_t *inst) {
    int status;
    pid_t rc;

    if (!inst || inst->daemon_pid <= 0) return;

    rc = waitpid(inst->daemon_pid, &status, WNOHANG);
    if (rc == inst->daemon_pid) {
        inst->daemon_pid = -1;
        inst->daemon_running = false;
        set_error(inst, "shairport-sync exited unexpectedly");
    }
}

/* --- FIFO management --- */

static int create_fifo(airplay_instance_t *inst) {
    if (!inst) return -1;

    snprintf(inst->fifo_path, sizeof(inst->fifo_path),
             "/tmp/airplay-audio-%d", inst->slot);

    /* Remove stale FIFO if it exists */
    (void)unlink(inst->fifo_path);

    if (mkfifo(inst->fifo_path, 0666) != 0) {
        set_error(inst, "mkfifo failed");
        return -1;
    }

    /*
     * Open O_RDWR | O_NONBLOCK: keeps a write reference open so we never
     * get spurious EOF when shairport-sync hasn't connected yet or
     * disconnects between AirPlay sessions. Reads return EAGAIN when
     * no data is available.
     */
    inst->fifo_fd = open(inst->fifo_path, O_RDWR | O_NONBLOCK);
    if (inst->fifo_fd < 0) {
        set_error(inst, "failed to open audio FIFO");
        (void)unlink(inst->fifo_path);
        return -1;
    }

    return 0;
}

static void close_fifo(airplay_instance_t *inst) {
    if (!inst) return;

    if (inst->fifo_fd >= 0) {
        close(inst->fifo_fd);
        inst->fifo_fd = -1;
    }

    if (inst->fifo_path[0] != '\0') {
        (void)unlink(inst->fifo_path);
        inst->fifo_path[0] = '\0';
    }
}

/* --- Pipe pump (reads FIFO into ring buffer) --- */

static void pump_pipe(airplay_instance_t *inst) {
    uint8_t buf[4096];
    uint8_t merged[4100];
    int16_t samples[2048];

    if (!inst || inst->fifo_fd < 0) return;

    while (1) {
        if (ring_available(inst) + 2048 >= (size_t)RING_SAMPLES) {
            break;
        }

        ssize_t n = read(inst->fifo_fd, buf, sizeof(buf));
        if (n > 0) {
            size_t merged_bytes = inst->pending_len;
            size_t aligned_bytes;
            size_t remainder;
            size_t sample_count;

            if (inst->pending_len > 0) {
                memcpy(merged, inst->pending_bytes, inst->pending_len);
            }

            memcpy(merged + merged_bytes, buf, (size_t)n);
            merged_bytes += (size_t)n;

            aligned_bytes = merged_bytes & ~((size_t)3U);
            remainder = merged_bytes - aligned_bytes;
            if (remainder > 0) {
                memcpy(inst->pending_bytes, merged + aligned_bytes, remainder);
            }
            inst->pending_len = (uint8_t)remainder;

            sample_count = aligned_bytes / sizeof(int16_t);
            if (sample_count > 0) {
                memcpy(samples, merged, sample_count * sizeof(int16_t));
                ring_push(inst, samples, sample_count);
            }

            inst->last_audio_ms = now_ms();
            inst->receiving_audio = true;

            if ((size_t)n < sizeof(buf)) {
                break;
            }
            continue;
        }

        if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
            break;
        }

        /* Unexpected error */
        break;
    }

    /* Mark idle if no audio received recently */
    if (inst->receiving_audio && inst->last_audio_ms > 0) {
        uint64_t now = now_ms();
        if (now > inst->last_audio_ms && (now - inst->last_audio_ms) > AUDIO_IDLE_MS) {
            inst->receiving_audio = false;
        }
    }
}

/* --- Plugin API v2 --- */

static void* v2_create_instance(const char *module_dir, const char *json_defaults) {
    airplay_instance_t *inst;

    inst = calloc(1, sizeof(*inst));
    if (!inst) return NULL;

    inst->slot = ++g_instance_counter;

    snprintf(inst->module_dir, sizeof(inst->module_dir), "%s",
             module_dir ? module_dir : ".");
    snprintf(inst->device_name, sizeof(inst->device_name),
             "Move - Slot %d", inst->slot);
    snprintf(inst->config_path, sizeof(inst->config_path),
             "/tmp/airplay-config-%d.conf", inst->slot);

    inst->gain = 1.0f;
    inst->fifo_fd = -1;
    inst->daemon_pid = -1;

    (void)json_defaults;

    if (create_fifo(inst) != 0) {
        free(inst);
        return NULL;
    }

    if (start_daemon(inst) != 0) {
        close_fifo(inst);
        free(inst);
        return NULL;
    }

    ap_log("airplay plugin instance created");
    return inst;
}

static void v2_destroy_instance(void *instance) {
    airplay_instance_t *inst = (airplay_instance_t *)instance;
    if (!inst) return;

    stop_daemon(inst);
    close_fifo(inst);

    if (inst->config_path[0] != '\0') {
        (void)unlink(inst->config_path);
    }

    free(inst);
    if (g_instance_counter > 0) g_instance_counter--;
    ap_log("airplay plugin instance destroyed");
}

static void v2_on_midi(void *instance, const uint8_t *msg, int len, int source) {
    (void)instance;
    (void)msg;
    (void)len;
    (void)source;
}

static void v2_set_param(void *instance, const char *key, const char *val) {
    airplay_instance_t *inst = (airplay_instance_t *)instance;
    if (!inst || !key || !val) return;

    if (strcmp(key, "rec_source_mode") == 0) {
        inst->rec_source_mode = (strcmp(val, "1") == 0);
        return;
    }

    if (strcmp(key, "gain") == 0) {
        float g = (float)atof(val);
        if (g < 0.0f) g = 0.0f;
        if (g > 2.0f) g = 2.0f;
        inst->gain = g;
        return;
    }

    if (strcmp(key, "device_name") == 0) {
        char log_msg[256];
        if (val[0] == '\0') return;
        snprintf(inst->device_name, sizeof(inst->device_name), "%s", val);
        snprintf(log_msg, sizeof(log_msg), "device name changed to: %s", inst->device_name);
        ap_log(log_msg);
        /* Restart daemon with new name */
        clear_ring(inst);
        start_daemon(inst);
        return;
    }

    if (strcmp(key, "pause") == 0) {
        inst->paused = true;
        return;
    }

    if (strcmp(key, "resume") == 0) {
        inst->paused = false;
        return;
    }

    if (strcmp(key, "play_pause_toggle") == 0) {
        inst->paused = !inst->paused;
        return;
    }

    if (strcmp(key, "restart") == 0) {
        ap_log("manual restart requested");
        clear_ring(inst);
        clear_error(inst);
        inst->receiving_audio = false;
        start_daemon(inst);
        return;
    }
}

static int v2_get_param(void *instance, const char *key, char *buf, int buf_len) {
    airplay_instance_t *inst = (airplay_instance_t *)instance;
    if (!key || !buf || buf_len <= 0) return -1;

    if (strcmp(key, "audio_level") == 0) {
        return snprintf(buf, (size_t)buf_len, "%.3f", inst ? inst->peak_level : 0.0f);
    }

    if (strcmp(key, "gain") == 0) {
        return snprintf(buf, (size_t)buf_len, "%.2f", inst ? inst->gain : 1.0f);
    }

    if (strcmp(key, "preset_name") == 0 || strcmp(key, "name") == 0) {
        return snprintf(buf, (size_t)buf_len, "AirPlay");
    }

    if (strcmp(key, "device_name") == 0) {
        return snprintf(buf, (size_t)buf_len, "%s",
                        inst ? inst->device_name : "Move");
    }

    if (strcmp(key, "status") == 0) {
        if (!inst) return snprintf(buf, (size_t)buf_len, "error");
        if (inst->error_msg[0] != '\0') return snprintf(buf, (size_t)buf_len, "error");
        if (!inst->daemon_running) return snprintf(buf, (size_t)buf_len, "stopped");
        if (inst->receiving_audio) return snprintf(buf, (size_t)buf_len, "playing");
        return snprintf(buf, (size_t)buf_len, "waiting");
    }

    if (strcmp(key, "playback_active") == 0) {
        if (!inst) return snprintf(buf, (size_t)buf_len, "0");
        if (inst->receiving_audio) return snprintf(buf, (size_t)buf_len, "1");
        return snprintf(buf, (size_t)buf_len, "0");
    }

    return -1;
}

static int v2_get_error(void *instance, char *buf, int buf_len) {
    airplay_instance_t *inst = (airplay_instance_t *)instance;
    if (!inst || !inst->error_msg[0]) return 0;
    return snprintf(buf, (size_t)buf_len, "%s", inst->error_msg);
}

static void v2_render_block(void *instance, int16_t *out_interleaved_lr, int frames) {
    airplay_instance_t *inst = (airplay_instance_t *)instance;
    size_t needed;
    size_t got;
    size_t i;

    if (!out_interleaved_lr || frames <= 0) return;

    needed = (size_t)frames * 2;
    memset(out_interleaved_lr, 0, needed * sizeof(int16_t));

    if (!inst) return;

    /* Periodically check if shairport-sync is still alive */
    check_daemon_alive(inst);

    /* Read audio data from FIFO into ring buffer */
    pump_pipe(inst);

    /* Pop audio from ring buffer */
    got = ring_pop(inst, out_interleaved_lr, needed);

    /* If paused, silence the output (connection stays alive, FIFO is still
     * drained above so shairport-sync doesn't block). */
    if (inst->paused) {
        memset(out_interleaved_lr, 0, needed * sizeof(int16_t));
        got = 0;
    }

    /* Apply gain */
    if (inst->gain != 1.0f && got > 0) {
        for (i = 0; i < got; i++) {
            float s = out_interleaved_lr[i] * inst->gain;
            if (s > 32767.0f) s = 32767.0f;
            if (s < -32768.0f) s = -32768.0f;
            out_interleaved_lr[i] = (int16_t)s;
        }
    }

    /* Update peak level for rec source monitoring */
    {
        float peak = 0.0f;
        for (i = 0; i < needed; i++) {
            float s = (float)abs(out_interleaved_lr[i]) / 32768.0f;
            if (s > peak) peak = s;
        }
        inst->peak_level = peak;
    }

    /* Prevent the host's idle gate from sleeping this slot while the daemon
     * is running.  AirPlay audio arrives asynchronously from the network so
     * there can be long periods of silence between connections.  A single
     * sample above the silence threshold (DSP_SILENCE_LEVEL = 4) keeps the
     * render loop active so pump_pipe() drains the FIFO in real-time.
     * This must be unconditional — after a track ends, the slot goes silent
     * and the idle gate would kick in before the next track starts. */
    if (inst->daemon_running) {
        out_interleaved_lr[needed - 1] |= 5;
    }
}

static plugin_api_v2_t g_plugin_api_v2 = {
    .api_version       = MOVE_PLUGIN_API_VERSION_2,
    .create_instance   = v2_create_instance,
    .destroy_instance  = v2_destroy_instance,
    .on_midi           = v2_on_midi,
    .set_param         = v2_set_param,
    .get_param         = v2_get_param,
    .get_error         = v2_get_error,
    .render_block      = v2_render_block,
};

plugin_api_v2_t* move_plugin_init_v2(const host_api_v1_t *host) {
    g_host = host;
    ap_log("airplay plugin v2 initialized");
    return &g_plugin_api_v2;
}
