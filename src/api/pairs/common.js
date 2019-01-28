// @flow
const {
  DURATION_MS,
  KAFKA_BROKERS,
  STEP_DELAY_MS,
} = process.env;

const pureDuration = parseInt(DURATION_MS) || 86400000;
const pureStepDelay = parseInt(STEP_DELAY_MS) || 1000;

if (pureDuration < pureStepDelay) {
  throw new Error('Statistics step delay can\'t be great than duration');
}

const KAFKA_BROKERS_LIST = (function getPureKfkaBrokers() {
  const emptyStr = '';
  const brokerList = typeof KAFKA_BROKERS === 'string' ? KAFKA_BROKERS.replace(' ', emptyStr) : emptyStr;
  const brokerListArr = brokerList.split(',');
  const brokerListLength = brokerListArr.length;

  if (brokerListLength === 0) {
    throw new Error('Settings option "KAFKA_BROKERS" is required');
  }

  const urlRegExp = /^(https?:\/\/)?(?:[^@/\n]+@)?(?:www\.)?([^:/\n]+)((?::\d+)?)$/i;
  let i = 0;

  for (i; i < brokerListLength; i += 1) {
    const broker = brokerListArr[i];

    if (!urlRegExp.test(broker)) {
      throw new Error('Settings option "KAFKA_BROKERS" is incorrect.(Ex: 127.0.0.1:9092,127.0.0.1:9093)');
    }
  }

  return brokerList;
}());

const ACTIONS = {
  ADD: 'ADD',
  DUMP: 'DUMP',
  TICK: 'TICK',
  REMOVE: 'REMOVE',
};

const PAIRS_FILE_PATH = `${DATA_PATH}/pairs`;
const PAIR_FILES_DIR_PATH = `${DATA_PATH}/pair-files`;

const COMMON_DATA = {
  ACTIONS,
  PAIRS_FILE_PATH,
  PAIR_FILES_DIR_PATH,
  KAFKA_BROKERS_LIST,
  DURATION: pureDuration,
  STEP_DELAY: pureStepDelay,
};

module.exports = COMMON_DATA;
