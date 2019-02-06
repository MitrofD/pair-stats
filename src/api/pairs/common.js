// @flow
const ACTIONS = {
  ADD: 'ADD',
  DUMP: 'DUMP',
  TICK: 'TICK',
  REMOVE: 'REMOVE',
};

const PAIRS_FILE_PATH = `${__dirname}/pairs.list`;
const PAIR_DUMPS_DIR_PATH = `${__dirname}/pair-dumps`;

const COMMON_DATA = {
  ACTIONS,
  PAIRS_FILE_PATH,
  PAIR_DUMPS_DIR_PATH,
};

module.exports = COMMON_DATA;
