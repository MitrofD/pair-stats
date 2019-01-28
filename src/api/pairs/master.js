// @flow
const cluster = require('cluster');
const { KafkaConsumer } = require('node-rdkafka');
const fs = require('fs');
const common = require('./common');

type ProcessAction = Object & {
  action: string,
};

const has = Object.prototype.hasOwnProperty;

let VALS: { [string]: string[] } = {};
const WORKERS_ARR: Object[] = [];
const PAIRS_REL_WORKER: { [string]: Object } = {};
const WORKER_REL_PAIRS_COUNT: { [string]: number } = {};

const sWorkerIds = Object.keys(cluster.workers);
const sWorkersLength = sWorkerIds.length;

if (sWorkersLength === 0) {
  throw new Error('Not found workers');
}

let sWI = 0;

for (sWI; sWI < sWorkersLength; sWI += 1) {
  const workerId = sWorkerIds[sWI];
  const worker = cluster.workers[workerId];
  WORKERS_ARR.push(worker);
  WORKER_REL_PAIRS_COUNT[workerId] = 0;
}

const createPairFilesDir = (): Promise<void> => {
  const createPromise = new Promise((resolve, reject) => {
    fs.stat(common.PAIR_FILES_DIR_PATH, (error, stat) => {
      const mkDirCallback: Function = (mDError) => {
        if (mDError) {
          reject(mDError);
          return;
        }

        resolve();
      };

      if (error) {
        if (error.code === 'ENOENT') {
          fs.mkdir(common.PAIR_FILES_DIR_PATH, mkDirCallback);
          return;
        }

        reject(error);
        return;
      }

      if (!stat.isDirectory()) {
        fs.mkdir(common.PAIR_FILES_DIR_PATH, mkDirCallback);
        return;
      }

      resolve();
    });
  });

  return createPromise;
};

const getPairs = (): Promise<string[]> => {
  const readFile = (clbck: Function) => {
    fs.readFile(common.PAIRS_FILE_PATH, 'utf8', clbck);
  };

  const strAsPairsArr = (str: string) => {
    const pureStr = str.trim();
    const pairArr = pureStr.split('\n');
    const rArr = pairArr.map((pair) => pair.trim());
    return rArr;
  };

  const getPromise = new Promise((resolve, reject) => {
    readFile((rError, rData) => {
      if (rError && rError.code !== 'ENOENT') {
        reject(rError);
        return;
      }

      if (rData) {
        const pairsArray = strAsPairsArr(rData);
        resolve(pairsArray);
        return;
      }

      fs.open(common.PAIRS_FILE_PATH, 'w+', (cError, fd) => {
        if (cError) {
          reject(cError);
          return;
        }

        fs.close(fd, () => {
          readFile((rAError, rAData) => {
            if (rAError) {
              reject(rAError);
              return;
            }

            const pairsArray = strAsPairsArr(rAData);
            resolve(pairsArray);
          });
        });
      });
    });
  });

  return getPromise;
};

const savePairs = (pairsArr: string[]): Promise<void> => {
  const savePromise = new Promise((resolve, reject) => {
    fs.writeFile(common.PAIRS_FILE_PATH, pairsArr.join('\n'), (error) => {
      if (error) {
        reject(error);
        return;
      }

      resolve();
    });
  });

  return savePromise;
};

const getPairsPromise = getPairs();
const createPairFilesDirPromise = createPairFilesDir();
const createFileAndDirIdNeeded = Promise.all([
  getPairsPromise,
  createPairFilesDirPromise,
]);

const getNextWorker = () => {
  const workerIds = Object.keys(cluster.workers);
  const workerLength = workerIds.length;

  let minCount = Number.POSITIVE_INFINITY;
  let id = workerIds[0];
  let i = 0;

  for (i; i < workerLength; i += 1) {
    const workerId = workerIds[i];

    if (!has.call(WORKER_REL_PAIRS_COUNT, workerId)) {
      continue;
    }

    const pairsCount = WORKER_REL_PAIRS_COUNT[workerId];

    if (pairsCount === 0) {
      return cluster.workers[workerId];
    }

    if (minCount < pairsCount) {
      id = workerId;
      minCount = pairsCount;
    }
  }

  return cluster.workers[id];
};

const getPairsArr = () => Object.keys(PAIRS_REL_WORKER);

const addPair = (pair: string, save = false) => {
  const purePair = pair.trim();
  const existsPairs = getPairsArr();

  if (existsPairs.includes(purePair) || purePair.length === 0) {
    return;
  }

  const worker = getNextWorker();
  PAIRS_REL_WORKER[purePair] = worker;
  VALS[purePair] = [];
  WORKER_REL_PAIRS_COUNT[purePair] += 1;

  const finish = () => {
    worker.send({
      pair: purePair,
      action: common.ACTIONS.ADD,
    });
  };

  if (save) {
    existsPairs.push(purePair);
    savePairs(existsPairs).then(finish).catch(globErrorHandler);
  } else {
    finish();
  }
};

const removePair = (pair: string) => {
  const purePair = pair.trim();
  const existsPairs = getPairsArr();
  const pairIndex = existsPairs.indexOf(purePair);

  if (pairIndex === -1 || purePair.length === 0) {
    return;
  }

  existsPairs.splice(pairIndex, 1);
  const worker = PAIRS_REL_WORKER[purePair];

  savePairs(existsPairs).then(() => {
    worker.send({
      pair: purePair,
      action: common.ACTIONS.REMOVE,
    });

    delete PAIRS_REL_WORKER[purePair];
    delete VALS[purePair];
    WORKER_REL_PAIRS_COUNT[purePair] -= 1;
  }).catch(globErrorHandler);
};

const sendActionToAllWorkers = (data: ProcessAction) => {
  let i = 0;
  const workersLength = WORKERS_ARR.length;

  for (i; i < workersLength; i += 1) {
    const worker = WORKERS_ARR[i];
    worker.send(data);
  }
};

const KAFKA_GROUP_ID = 'pair-stats';

const pairActionConsumer = (function makePairActionConsumer() {
  const topic = 'pair-action';
  let consumer: ?Object = null;

  return Object.freeze({
    start() {
      this.stop();

      const newConsumer = new KafkaConsumer({
        'enable.auto.commit': true,
        'group.id': KAFKA_GROUP_ID,
        'metadata.broker.list': common.KAFKA_BROKERS_LIST,
      });

      newConsumer.on('ready', () => {
        newConsumer.subscribe([
          topic,
        ]);

        newConsumer.consume();
      });

      newConsumer.on('data', (data) => {
        const action = data.value.toString();
        const pureAction = action.trim().toUpperCase();
        const pair = data.key.toString();

        if (pureAction === common.ACTIONS.ADD) {
          addPair(pair, true);
          return;
        }

        if (pureAction === common.ACTIONS.REMOVE) {
          removePair(pair);
        }
      });

      newConsumer.connect();
      consumer = newConsumer;
    },

    stop() {
      if (typeof consumer === 'object' && consumer !== null) {
        consumer.disconnect();
        consumer = null;
      }
    },
  });
}());

const pairPriceSizeConsumer = (function makePairPriceSizeConsumer() {
  const topic = 'pair-price-size';
  let consumer: ?Object = null;

  return Object.freeze({
    start() {
      this.stop();

      const newConsumer = new KafkaConsumer({
        'auto.commit.interval.ms': common.STEP_DELAY,
        'enable.auto.commit': true,
        'group.id': KAFKA_GROUP_ID,
        'metadata.broker.list': common.KAFKA_BROKERS_LIST,
      });

      newConsumer.on('ready', () => {
        newConsumer.subscribe([
          topic,
        ]);

        newConsumer.consume();
      });

      newConsumer.on('data', (data) => {
        const pairName = data.key;

        if (!has.call(VALS, pairName)) {
          return;
        }

        const message = data.value.toString();
        const messParts = message.split(' ');
        VALS[pairName].push(messParts);
      });

      newConsumer.connect();
      consumer = newConsumer;
    },

    stop() {
      if (typeof consumer === 'object' && consumer !== null) {
        consumer.disconnect();
        consumer = null;
      }
    },
  });
}());

const messCron = (function makeMessCron() {
  const dumpStep = 10000;
  let dumpTimeoutID: ?TimeoutID = null;
  let tickTimeoutID: ?TimeoutID = null;

  const dump = (msDelay: number) => {
    dumpTimeoutID = setTimeout(() => {
      sendActionToAllWorkers({
        action: common.ACTIONS.DUMP,
      });

      dump(dumpStep);
    }, msDelay);
  };

  const tick = (msDelay: number) => {
    tickTimeoutID = setTimeout(() => {
      sendActionToAllWorkers({
        action: common.ACTIONS.TICK,
        data: VALS,
      });

      VALS = {};
      const pairsArr = getPairsArr();
      const pairsArrLength = pairsArr.length;
      let i = 0;

      for (i; i < pairsArrLength; i += 1) {
        const pair = pairsArr[i];
        VALS[pair] = [];
      }

      tick(common.STEP_DELAY);
    }, msDelay);
  };

  const stopTimeoutIfNeeded = (timeoutID: ?TimeoutID) => {
    if (timeoutID !== null) {
      clearTimeout(timeoutID);
      // eslint-disable-next-line no-param-reassign
      timeoutID = null;
    }
  };

  return Object.freeze({
    start() {
      const nowTime = Date.now();
      const needDumpDelay = dumpStep - (nowTime % dumpStep);
      const needTickDelay = common.STEP_DELAY - (nowTime % common.STEP_DELAY);
      dump(needDumpDelay);
      tick(needTickDelay);
      pairActionConsumer.start();
      pairPriceSizeConsumer.start();
    },

    stop() {
      stopTimeoutIfNeeded(dumpTimeoutID);
      stopTimeoutIfNeeded(tickTimeoutID);
      pairActionConsumer.stop();
      pairPriceSizeConsumer.stop();
      VALS = {};
    },
  });
}());

let initPairs: Array<string> = [];
let workersDidInit = false;
let fileStructDidInit = false;

const sWorkersIfNeeded = () => {
  if (workersDidInit && fileStructDidInit) {
    initPairs.forEach(addPair);
    messCron.start();
  }
};

createFileAndDirIdNeeded.then(([pairs]) => {
  fileStructDidInit = true;
  initPairs = pairs;
  sWorkersIfNeeded();
}).catch((error) => {
  throw error;
});

(function initWorkers() {
  let currCount = 0;
  const eventName = 'online';

  function onlineFunc() {
    this.off(eventName, onlineFunc);
    currCount += 1;

    if (currCount === sWorkersLength) {
      workersDidInit = true;
      sWorkersIfNeeded();
    }
  }

  let i = 0;
  const workersLength = WORKERS_ARR.length;

  for (i; i < workersLength; i += 1) {
    const worker = WORKERS_ARR[i];
    worker.on(eventName, onlineFunc);
  }
}());
