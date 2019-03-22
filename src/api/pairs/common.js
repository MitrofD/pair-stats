// @flow
const ACTION = {
  ADD_PAIR: 'ADD_PAIR',
  DUMP: 'DUMP',
  TICK: 'TICK',
  REMOVE_PAIR: 'REMOVE_PAIR',
};

const PAIRS_FILE_PATH = `${__dirname}/pairs.list`;
const PAIR_DUMPS_DIR_PATH = `${__dirname}/dumps`;
const NAME = 'pair-stats';

module.exports = {
  ACTION,
  NAME,
  PAIRS_FILE_PATH,
  PAIR_DUMPS_DIR_PATH,
};
