// @flow
const MS_IN_SEC = 1000;
const STATISTICS_DURATION = 86400 * MS_IN_SEC;
const STATISTICS_STEP_DELAY = 2.5 * MS_IN_SEC;

if (STATISTICS_DURATION < STATISTICS_STEP_DELAY) {
  throw new Error('Statistics step delay can\'t be great than duration');
}

const ACTIONS = {
  ADD_PAIR: 'ADD_PAIR',
  TICK: 'TICK',
  REMOVE_PAIR: 'REMOVE_PAIR',
};

const COMMON_DATA = {
  ACTIONS,
  STATISTICS_DURATION,
  STATISTICS_STEP_DELAY,
  FILES_PATH: `${DATA_PATH}/pairs`,
};

module.exports = COMMON_DATA;
