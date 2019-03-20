// @flow
const ACTION = {
  ADD_PAIR: 'ADD_PAIR',
  DUMP: 'DUMP',
  FORCE_TICK: 'FORCE_TICK',
  TICK: 'TICK',
  REMOVE_PAIR: 'REMOVE_PAIR',
};

const pwd = __dirname;
const PAIRS_FILE_PATH = `${pwd}/pairs.list`;
const PAIR_DUMPS_DIR_PATH = `${pwd}/dumps`;
const NAME = 'pair-stats';

module.exports = {
  ACTION,
  NAME,
  PAIRS_FILE_PATH,
  PAIR_DUMPS_DIR_PATH,
};
