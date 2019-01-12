// @flow
const cluster = require('cluster');
const fs = require('fs');
const common = require('./common');
const tools = require('../tools');

/* ITEM STRUCTURE: [time, max, prevMax, min, prevMin, size, totalSize] */
/*
type Value = number[];
type Values = Value[];

const MAX_VALS: { [string]: Values } = {};
const MIN_VALS: { [string]: Values } = {};
const SIZE_VALS: { [string]: Values } = {};

const getPairFilePath = (pair: string) => `${common.FILES_PATH}/${pair}`;

const getValuesBeforeTime = (vals: Values, time: number): Values => {
  const rVals: Values = [];
  const valsLength = vals.length;

  for (let i = 0; i < valsLength; i += 1) {
    const val = rVals[i];
    const valTime = val[0];

    if (valTime < time) {
      break;
    }

    rVals.push(val);
  }

  return rVals;
};

const getInitStatsForPair = (pair: string): Promise<Object> => {
  const savePath = getPairFilePath(pair);

  const getPromise = new Promise((resolve, reject) => {
    fs.readFile(savePath, (error, data) => {
      if (error) {
        reject(error);
        return;
      }

      let rObj = {};

      try {
        // eslint-disable-next-line flowtype-errors/show-errors
        const json = JSON.parse(data);

        if (typeof json === 'object' && json !== null) {
          rObj = json;
        }
        // eslint-disable-next-line no-empty
      } catch (parseError) {}

      const beforeTime = Date.now() - common.DURATION;

      const reqAttrs = [
        'MAX_VALS',
        'MIN_VALS',
        'SIZE_VALS',
      ];

      reqAttrs.forEach((attr) => {
        const checkArr: Values = Array.isArray(rObj[attr]) ? rObj[attr] : [];
        rObj[attr] = getValuesBeforeTime(checkArr, beforeTime);
      });

      resolve(rObj);
    });
  });

  return getPromise;
};

const getDataFromRow = (strArr: string[]): number[] => {
  let totalSize = 0;
  let max = Number.NEGATIVE_INFINITY;
  let min = Number.POSITIVE_INFINITY;
  const arrLength = strAr.length;

  for (let i = 0; i < arrLength; i += 1) {
    const price = +strArr[0];
    const size = +strArr[1];
    totalSize += size;

    if (price > max) {
      max = price;
    }

    if (price < min) {
      min = price;
    }
  }

  return [
    max,
    min,
    totalSize,
  ];
};

const addPair = (pair: string) => {
  if (tools.has.call(STATS, pair)) {
    return;
  }

  getInitStatsForPair(pair).then((stats) => {
    MAX_VALS[pair] = stats;
    VALS[pair] = getStatByIdx(pair, 0);
  }).catch(ErrorHandler);
};

const tick = (data: { [string]: string[] }) => {
  const timeNow = Date.now();
  const maxRangeTime = timeNow - common.DURATION;
  const availablePairs = Object.keys(MAX_VALS);
  const pLength = availablePairs.length;

  for (let i = 0; i < pLength; i += 1) {
    const pair = availablePairs[i];
    const pairVal = VALS[pair];
    /*
    const stats = STATS[pair];
    const statsLength = stats.length;
    const lastVal = getStatByIdx(pair, statsLength - 1);
    */

/*
    if (lastVal[0] >= maxRangeTime) {

      break;
    }

    if (NEED_COUNT === statsLength) {

      const prevVal = getStatByIdx(pair, 1);
      pairVal.tSize -= lastVal.size;

      if (lastVal.max === pairVal.max) {
        pairVal.max
      }
    }

    if (tools.has.call(data, pair)) {
      const currPairData = getDataFromRow(pair);
    }
    */
/*
  }
};

const pairExists = (pair: string) => (tools.has.call(MAX_VALS, pair));

const removePair = (pair: string) => {
  delete MAX_VALS[pair];
  delete MIN_VALS[pair];
  delete SIZE_VALS[pair];

  const filePath = getPairFilePath(pair);
  fs.unlink(filePath, () => {});
};

const savePair = (pair: string): Promise<void> => {
  if (!pairExists(pair)) {
    const saveError = new Error(`Pair "${pair}" not available`);
    return Promise.reject(saveError);
  }

  const savePath = getPairFilePath(pair);
  const saveData = {
    MAX_VALS: MAX_VALS[pair],
    MIN_VALS: MIN_VALS[pair],
    SIZE_VALS: SIZE_VALS[pair],
  };

  const saveData = JSON.stringify(saveData);

  const savePromise = new Promise((resolve, reject) => {
    fs.writeFile(savePath, saveData, (error) => {
      if (error) {
        reject(error);
        return;
      }

      resolve();
    });
  });

  return savePromise;
};

process.on('message', (function makeMessHandler() {
  const actions = {
    [common.ACTIONS.TICK](actionData: Object) {
      tick(actionData.data);
    },

    [common.ACTIONS.ADD_PAIR](actionData: Object) {
      addPair(actionData.pair);
    },

    [common.ACTIONS.REMOVE_PAIR](actionData: Object) {
      removePair(actionData.pair);
    },
  };

  return (data) => {
    if (tools.has.call(actions, data.action)) {
      actions[data.action](data);
    }
  };
}()));
*/

/*
(function startKafkaInit() {
  const {
    KAFKA_BROKERS,
  } = process.env;

  const emptyStr = '';
  const kafkaBrokerListStr = typeof KAFKA_BROKERS === 'string' ? KAFKA_BROKERS.replace(' ', emptyStr) : emptyStr;
  const kafkaBrokerListArr = kafkaBrokerListStr.split(',');

  if (kafkaBrokerListArr.length === 0) {
    throw new Error('Settings option "KAFKA_BROKERS" is required');
  }

  for (let i = 0; i < kafkaBrokerListArr.length; i += 1) {
    const kafkaBroker = kafkaBrokerListArr[i];

    if (!tools.urlRegExp.test(kafkaBroker)) {
      throw new Error('Settings option "KAFKA_BROKERS" is incorrect.(Ex: 127.0.0.1:9092,127.0.0.1:9093)');
    }
  }

  const consumer = new KafkaConsumer({
    'group.id': `stats-${cluster.worker.id}`,
    'metadata.broker.list': kafkaBrokerListStr,
  });

  consumer.on('ready', () => {
    consumer.subscribe([
      'pair-transaction',
    ]);

    consumer.consume();
  }).on('data', (data) => {
    const mess = data.value.toString();
    const messParts = mess.split(' ');
    const pairName = messParts[0];
    // console.log(`stats-${cluster.worker.id}`, pairName);

    if (tools.has.call(TMP_VALS, pairName)) {
      const tmpVal = TMP_VALS[pairName];
      const price = +messParts[1];
      const size = +messParts[2];
      tmpVal.size += size;

      if (price > tmpVal.max) {
        tmpVal.max = price;
      }

      if (price < tmpVal.min) {
        tmpVal.min = price;
      }
    }
  });

  consumer.connect();
}());
*/
