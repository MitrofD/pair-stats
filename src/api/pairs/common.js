// @flow
const {
  DURATION_MS,
  STEP_DELAY_MS,
} = process.env;

const pureDuration = parseInt(DURATION_MS) || 86400000;
const pureStepDelay = parseInt(STEP_DELAY_MS) || 1000;

if (pureDuration < pureStepDelay) {
  throw new Error('Statistics step delay can\'t be great than duration');
}

const ACTIONS = {
  ADD_PAIR: 'ADD_PAIR',
  DUMP: 'DUMP',
  TICK: 'TICK',
  REMOVE_PAIR: 'REMOVE_PAIR',
};

const PAIRS_FILE_PATH = `${DATA_PATH}/pairs`;
const PAIR_FILES_DIR_PATH = `${DATA_PATH}/pair-files`;

const COMMON_DATA = {
  ACTIONS,
  PAIRS_FILE_PATH,
  PAIR_FILES_DIR_PATH,
  DURATION: pureDuration,
  STEP_DELAY: pureStepDelay,
};

module.exports = COMMON_DATA;
