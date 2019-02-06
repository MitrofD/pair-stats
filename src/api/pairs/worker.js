// @flow
const fs = require('fs');
const { Producer } = require('node-rdkafka');
const common = require('./common');

// Item structure: [time, price, max, min, size]

type Value = number[];
type Values = Value[];

const STATS_TOPIC_NAME = 'pair-stats';

const producer = new Producer({
  dr_cb: false,
  'metadata.broker.list': OPTIONS.KAFKA_BROKERS,
});

let messHandler: Function = () => {};

producer.on('ready', () => {
  const maxMessCounter = 6000;
  let messCounter = 0;

  messHandler = (pairName, mess) => {
    messCounter += 1;
    const buffMess = Buffer.from(mess);

    try {
      producer.produce(STATS_TOPIC_NAME, -1, buffMess, pairName);
      // eslint-disable-next-line no-empty
    } catch (error) {}

    if (messCounter > maxMessCounter) {
      producer.poll();
      messCounter = 0;
    }
  };
});

producer.connect();

const has = Object.prototype.hasOwnProperty;
const VALS_OBJ: { [string]: Values } = {};

const getFilePath = (pair: string) => `${common.PAIR_DUMPS_DIR_PATH}/${pair}.dmp`;

const pairExists = (pair: string) => has.call(VALS_OBJ, pair);

const getInitStatsForPair = (pair: string): Promise<Values> => {
  const filePath = getFilePath(pair);

  const getPromise = new Promise((resolve) => {
    fs.readFile(filePath, (error, data) => {
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
    VALS_OBJ[pair] = stats;
  }).catch(globPrintError);
};

const removePair = (pair: string) => {
  delete VALS_OBJ[pair];

  const filePath = getFilePath(pair);
  fs.unlink(filePath, (error) => {
    if (error) {
      globPrintError(error);
    }
  });
};

const savePair = (pair: string): Promise<void> => {
  if (!pairExists(pair)) {
    const saveError = new Error(`Pair "${pair}" not available`);
    return Promise.reject(saveError);
  }

  const savePath = getFilePath(pair);
  const saveDataStr = JSON.stringify(VALS_OBJ[pair]);

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

const dump = (function makeDumpFunc() {
  let IS_DUMP_MODE = false;

  return () => {
    if (IS_DUMP_MODE) {
      return;
    }

    IS_DUMP_MODE = true;

    setImmediate(() => {
      const promisesArray: Promise<void>[] = [];
      const availablePairs = Object.keys(VALS_OBJ);
      const pLength = availablePairs.length;
      let i = 0;

      for (; i < pLength; i += 1) {
        const pair = availablePairs[i];
        const savePromise = savePair(pair);
        promisesArray.push(savePromise);
      }

      Promise.all(promisesArray).then(() => {
        IS_DUMP_MODE = false;
      }).catch(() => {
        IS_DUMP_MODE = false;
      });
    });
  };
}());

const tick = (data: { [string]: string[] }) => {
  const timeNow = Date.now();
  const availablePairs = Object.keys(VALS_OBJ);
  const pLength = availablePairs.length;
  let i = 0;

  for (; i < pLength; i += 1) {
    const pair = availablePairs[i];
    const pairDataArr = VALS_OBJ[pair];

    // eslint-disable-next-line no-loop-func
    setImmediate(() => {
      const addData: string[] = Array.isArray(data[pair]) ? data[pair] : [];
      const addDataLength = addData.length;

      if (addDataLength > 0) {
        let addSize = 0;
        let addMax = 0;
        let addMin = Number.POSITIVE_INFINITY;
        let aI = 0;

        for (; aI < addDataLength; aI += 1) {
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

      const beforeTime = timeNow - OPTIONS.DURATION;
      const pairDataArrLength = pairDataArr.length;

      let price = 0;
      let max = 0;
      let min = Number.POSITIVE_INFINITY;
      let size = 0;
      let tI = 0;

      for (; tI < pairDataArrLength; tI += 1) {
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

      messHandler(pair, `${price} ${max} ${min} ${size} ${change} ${timeNow}`);
    });
  }
};

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

process.on('message', (data: ClusterMessage) => {
  const actionHandler = actions[data.action];

  if (typeof actionHandler === 'function') {
    actionHandler(data);
  }
});
