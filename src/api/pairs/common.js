// @flow
const ACTION = {
  ADD: 'ADD',
  DUMP: 'DUMP',
  FORCE_TICK: 'FORCE_TICK',
  TICK: 'TICK',
  REMOVE: 'REMOVE',
};

const PAIRS_FILE_PATH = `${__dirname}/pairs.list`;
const PAIR_DUMPS_DIR_PATH = `${__dirname}/dumps`;
const NAME = 'pair-stats';

const COMMON_DATA = {
  ACTION,
  NAME,
  PAIRS_FILE_PATH,
  PAIR_DUMPS_DIR_PATH,
};

module.exports = COMMON_DATA;
