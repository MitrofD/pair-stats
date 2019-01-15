// @flow
const fs = require('fs');
const common = require('./common');
const tools = require('../tools');

// Item structure: [time, price, max, min, size]

type Value = number[];
type Values = Value[];

const VALS: { [string]: Values } = {};

const getFilePath = (pair: string) => `${common.PAIR_FILES_DIR_PATH}/${pair}`;

const pairExists = (pair: string) => tools.has.call(VALS, pair);

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
  const purePair = pair.trim();

  if (pairExists(purePair)) {
    return;
  }

  getInitStatsForPair(purePair).then((stats) => {
    VALS[purePair] = stats;
  }).catch(ErrorHandler);
};

const removePair = (pair: string) => {
  const purePair = pair.trim();
  delete VALS[purePair];

  const filePath = getFilePath(pair);
  fs.unlink(filePath, ErrorHandler);
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
    savePair(pair).catch(ErrorHandler);
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

        const firstItem = addData[0];
        const price = +firstItem[0];

        pairDataArr.splice(0, 0, [
          timeNow,
          price,
          addMax,
          addMin,
          addSize,
        ]);
      }

      const beforeTime = timeNow - common.DURATION;
      const pairDataArrLength = pairDataArr.length;

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
        const diff = lastPrice - fItem[1];
        change = diff / lastPrice * 100;
      } else {
        min = 0;
      }

      console.log(pair, 'Max:', max, 'Min:', min, 'Size:', size, 'Change:', change);
    });
  }
};

process.on('message', (function makeMessHandler() {
  const actions = {
    [common.ACTIONS.TICK](actionData: Object) {
      tick(actionData.data);
    },

    [common.ACTIONS.DUMP]: dump,

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
