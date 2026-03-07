import {
  MoveKnob1, MoveKnob1Touch,
  MoveShift
} from '/data/UserData/move-anything/shared/constants.mjs';

import { isCapacitiveTouchMessage, decodeDelta } from '/data/UserData/move-anything/shared/input_filter.mjs';

import { createAction } from '/data/UserData/move-anything/shared/menu_items.mjs';
import { createMenuState, handleMenuInput } from '/data/UserData/move-anything/shared/menu_nav.mjs';
import { createMenuStack } from '/data/UserData/move-anything/shared/menu_stack.mjs';
import { drawStackMenu } from '/data/UserData/move-anything/shared/menu_render.mjs';

const SPINNER = ['-', '/', '|', '\\'];

let status = 'stopped';
let deviceName = 'Move';
let shiftHeld = false;

let menuState = createMenuState();
let menuStack = createMenuStack();

let tickCounter = 0;
let spinnerTick = 0;
let spinnerFrame = 0;
let needsRedraw = true;
var recSourceMode = false;
var recFlashCounter = 0;

function refreshState() {
  const prevStatus = status;
  status = host_module_get_param('status') || 'stopped';
  deviceName = host_module_get_param('device_name') || 'Move';

  if (prevStatus !== status) {
    rebuildMenu();
    needsRedraw = true;
  }
}

function statusLabel() {
  if (status === 'playing') return 'Receiving audio';
  if (status === 'waiting') return 'Waiting for connection';
  if (status === 'stopped') return 'Stopped';
  if (status === 'error') return 'Error';
  return status;
}

function buildRootItems() {
  const items = [];

  items.push(createAction(`Name: ${deviceName}`, () => {}));

  if (status === 'error' || status === 'stopped') {
    items.push(createAction('[Restart AirPlay]', () => {
      host_module_set_param('restart', '1');
      needsRedraw = true;
    }));
  }

  return items;
}

function rebuildMenu() {
  const items = buildRootItems();
  const current = menuStack.current();
  if (!current) {
    menuStack.push({
      title: 'AirPlay',
      items,
      selectedIndex: 0
    });
    menuState.selectedIndex = 0;
  } else {
    current.title = 'AirPlay';
    current.items = items;
    if (menuState.selectedIndex >= items.length) {
      menuState.selectedIndex = Math.max(0, items.length - 1);
    }
  }
  needsRedraw = true;
}

function currentFooter() {
  const activity = status === 'waiting' ? 'Waiting' : '';
  if (activity) return `${activity} ${SPINNER[spinnerFrame]}`;
  return statusLabel();
}

function drawRecSourceIndicator() {
  if (!recSourceMode) return;
  recFlashCounter++;

  var levelStr = host_module_get_param('audio_level');
  var level = levelStr ? parseFloat(levelStr) : 0;

  var labelText = 'REC:[AP]';
  var labelW = labelText.length * 6;
  var meterX = 128 - labelW - 5;
  var meterH = Math.round(level * 7);
  if (meterH > 0) {
    fill_rect(meterX, 2 + (7 - meterH), 2, meterH, 1);
  }

  var show = (recFlashCounter % 30) < 20;
  if (show) {
    print(128 - labelW, 2, labelText, 1);
  }
}

globalThis.init = function () {
  status = 'stopped';
  deviceName = 'Move';
  shiftHeld = false;

  menuState = createMenuState();
  menuStack = createMenuStack();
  tickCounter = 0;
  spinnerTick = 0;
  spinnerFrame = 0;
  needsRedraw = true;

  rebuildMenu();
};

globalThis.tick = function () {
  if (!recSourceMode) {
    var rsm = host_module_get_param('rec_source_mode');
    if (rsm === '1') recSourceMode = true;
  }

  tickCounter = (tickCounter + 1) % 6;
  if (tickCounter === 0) {
    refreshState();
  }

  if (status === 'waiting') {
    spinnerTick = (spinnerTick + 1) % 3;
    if (spinnerTick === 0) {
      spinnerFrame = (spinnerFrame + 1) % SPINNER.length;
      needsRedraw = true;
    }
  } else {
    spinnerTick = 0;
  }

  if (needsRedraw) {
    const current = menuStack.current();
    if (!current) {
      rebuildMenu();
    }

    clear_screen();
    drawStackMenu({
      stack: menuStack,
      state: menuState,
      footer: currentFooter()
    });
    drawRecSourceIndicator();

    needsRedraw = false;
  }
};

globalThis.onMidiMessageInternal = function (data) {
  const statusByte = data[0] & 0xF0;
  const cc = data[1];
  const val = data[2];

  if (isCapacitiveTouchMessage(data)) return;

  if (statusByte === 0xB0 && cc === MoveShift) {
    shiftHeld = val > 0;
    return;
  }

  if (statusByte !== 0xB0) return;

  const current = menuStack.current();
  if (!current) {
    rebuildMenu();
    return;
  }

  const result = handleMenuInput({
    cc,
    value: val,
    items: current.items,
    state: menuState,
    stack: menuStack,
    onBack: () => {
      host_return_to_menu();
    },
    shiftHeld
  });

  if (result.needsRedraw) {
    needsRedraw = true;
  }
};

globalThis.onMidiMessageExternal = function (data) {
  /* No external MIDI handling needed */
};

globalThis.chain_ui = {
  init: globalThis.init,
  tick: globalThis.tick,
  onMidiMessageInternal: globalThis.onMidiMessageInternal,
  onMidiMessageExternal: globalThis.onMidiMessageExternal
};
