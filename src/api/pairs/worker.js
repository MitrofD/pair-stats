// @flow
const fs = require('fs');
const { Producer } = require('node-rdkafka');
const common = require('./common');

// Item structure: [time, price, max, min, size]

type Value = number[];
type Values = Value[];

const topic = 'pair-stats';

const producer = new Producer({
  dr_cb: false,
  'metadata.broker.list': common.KAFKA_BROKERS_LIST,
});

let messHandler: Function = () => {};

producer.on('ready', () => {
  let messCounter = 0;
  const maxMessCounter = 60;

  messHandler = (pairName, data) => {
    messCounter += 1;
    const mess = JSON.stringify(data);
    const buffMess = Buffer.from(mess);

    try {
      producer.produce(topic, -1, buffMess, pairName);
      // eslint-disable-next-line no-empty
    } catch (error) {}

    if (messCounter > maxMessCounter) {
      messCounter = 0;
    }
  };
});

producer.connect();

const has = Object.prototype.hasOwnProperty;
const VALS: { [string]: Values } = {};

const getFilePath = (pair: string) => `${common.PAIR_FILES_DIR_PATH}/${pair}`;

const pairExists = (pair: string) => has.call(VALS, pair);

const getInitStatsForPair = (pair: string): Promise<Values> => {
  const pathFile = getFilePath(pair);

  const getPromise = new Promise((resolve) => {
    fs.readFile(pathFile, (error, data) => {
      let rArr: Values = [];

      if (data) {
        try {
          // eslint-disable-next-line flowtype-errors/show-errors
          const json = JSON.parse(data);

          if (Array.isArray(json)) {
            rArr = json;
          }
          // eslint-disable-next-line no-empty
        } catch (parseError) {}
      }

      resolve(rArr);
    });
  });

  return getPromise;
};

const addPair = (pair: string) => {
  if (pairExists(pair)) {
    return;
  }

  getInitStatsForPair(pair).then((stats) => {
    VALS[pair] = stats;
  }).catch(globErrorHandler);
};

const removePair = (pair: string) => {
  delete VALS[pair];

  const filePath = getFilePath(pair);
  fs.unlink(filePath, (error) => {
    if (error) {
      globErrorHandler(error);
    }
  });
};

const savePair = (pair: string): Promise<void> => {
  if (!pairExists(pair)) {
    const saveError = new Error(`Pair "${pair}" not available`);
    return Promise.reject(saveError);
  }

  const savePath = getFilePath(pair);
  const saveDataStr = JSON.stringify(VALS[pair]);

  const savePromise = new Promise((resolve, reject) => {
    fs.writeFile(savePath, saveDataStr, (error) => {
      if (error) {
        reject(error);
        return;
      }

      resolve();
    });
  });

  return savePromise;
};

const dump = () => {
  const availablePairs = Object.keys(VALS);
  const pLength = availablePairs.length;
  let i = 0;

  for (i; i < pLength; i += 1) {
    const pair = availablePairs[i];
    savePair(pair).catch(globErrorHandler);
  }
};

const tick = (data: { [string]: string[] }) => {
  const timeNow = Date.now();
  const availablePairs = Object.keys(VALS);
  const pLength = availablePairs.length;
  let i = 0;

  for (i; i < pLength; i += 1) {
    const pair = availablePairs[i];
    const pairDataArr = VALS[pair];

    // eslint-disable-next-line no-loop-func
    setImmediate(() => {
      const addData: string[] = Array.isArray(data[pair]) ? data[pair] : [];
      const addDataLength = addData.length;

      if (addDataLength > 0) {
        let addSize = 0;
        let addMax = 0;
        let addMin = Number.POSITIVE_INFINITY;
        let aI = 0;

        for (aI; aI < addDataLength; aI += 1) {
          const addDataItem = addData[aI];
          const price = +addDataItem[0];
          const size = +addDataItem[1];
          addSize += size;

          if (price > addMax) {
            addMax = price;
          }

          if (price < addMin) {
            addMin = price;
          }
        }

        const lastItem = addData[addDataLength - 1];
        const price = +lastItem[0];

        const addItem = [
          timeNow,
          price,
          addMax,
          addMin,
          addSize,
        ];

        pairDataArr.splice(0, 0, addItem);
      }

      const beforeTime = timeNow - common.DURATION;
      const pairDataArrLength = pairDataArr.length;

      let price = 0;
      let max = 0;
      let min = Number.POSITIVE_INFINITY;
      let size = 0;
      let tI = 0;

      for (tI; tI < pairDataArrLength; tI += 1) {
        const pairDataItem = pairDataArr[tI];

        if (pairDataItem[0] < beforeTime) {
          const sliceLength = pairDataArrLength - tI;
          pairDataArr.splice(tI, sliceLength);
          break;
        }

        size += pairDataItem[4];

        if (pairDataItem[2] > max) {
          max = pairDataItem[2];
        }

        if (pairDataItem[3] < min) {
          min = pairDataItem[3];
        }
      }

      const pairDataArrNewLength = pairDataArr.length;
      let change = 0;

      if (pairDataArrNewLength > 0) {
        const fItem = pairDataArr[0];
        const lItem = pairDataArr[pairDataArrNewLength - 1];
        const lastPrice = lItem[1];
        price = fItem[1];
        const diff = price - lastPrice;
        change = diff / lastPrice * 100;
      } else {
        min = 0;
      }

      messHandler(pair, {
        change,
        max,
        min,
        price,
        size,
      });
    });
  }
};

process.on('message', (function makeMessHandler() {
  const actions = {
    [common.ACTIONS.TICK](actionData: Object) {
      tick(actionData.data);
    },

    [common.ACTIONS.DUMP]: dump,

    [common.ACTIONS.ADD](actionData: Object) {
      addPair(actionData.pair);
    },

    [common.ACTIONS.REMOVE](actionData: Object) {
      removePair(actionData.pair);
    },
  };

  return (data) => {
    if (has.call(actions, data.action)) {
      actions[data.action](data);
    }
  };
}()));
