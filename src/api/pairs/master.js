// @flow
const cluster = require('cluster');
const { KafkaConsumer } = require('node-rdkafka');
const fs = require('fs');
const common = require('./common');

const has = Object.prototype.hasOwnProperty;

const createPairFilesDir = (): Promise<void> => {
  const createPromise = new Promise((resolve, reject) => {
    fs.stat(common.PAIR_DUMPS_DIR_PATH, (error, stat) => {
      const mkDirCallback: Function = (mDError) => {
        if (mDError) {
          reject(mDError);
          return;
        }

        resolve();
      };

      if (error) {
        if (error.code === 'ENOENT') {
          fs.mkdir(common.PAIR_DUMPS_DIR_PATH, mkDirCallback);
          return;
        }

        reject(error);
        return;
      }

      if (!stat.isDirectory()) {
        fs.mkdir(common.PAIR_DUMPS_DIR_PATH, mkDirCallback);
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

const KAFKA_GROUP_ID = 'pair-stats';
const PAIR_REL_WORKER_OBJ: { [string]: Object } = {};
const WORKER_REL_PAIR_OBJ: { [string]: string[] } = {};
const WORKER_REL_PAIRS_COUNT: { [string]: number } = {};
let VALS_OBJ: { [string]: string[] } = {};

const getPairsOfWorker = (workerId: string) => {
  let returnArr: string[] = [];
  const pairsOfWorker = WORKER_REL_PAIR_OBJ[workerId];

  if (Array.isArray(pairsOfWorker)) {
    returnArr = pairsOfWorker;
  }

  return returnArr;
};

const getNextWorker = () => {
  const workerIDs = Object.keys(cluster.workers);
  const workerLength = workerIDs.length;

  let minCount = Number.POSITIVE_INFINITY;
  let id = workerIDs[0];
  let i = 0;

  for (; i < workerLength; i += 1) {
    const workerID = workerIDs[i];

    if (!has.call(WORKER_REL_PAIRS_COUNT, workerID)) {
      continue;
    }

    const pairsCount = WORKER_REL_PAIRS_COUNT[workerID];

    if (pairsCount === 0) {
      return cluster.workers[workerID];
    }

    if (minCount > pairsCount) {
      id = workerID;
      minCount = pairsCount;
    }
  }

  return cluster.workers[id];
};

const getPairsArr = () => Object.keys(PAIR_REL_WORKER_OBJ);

const addPair = (pair: string, save = false) => {
  const purePair = pair.trim();
  const existsPairs = getPairsArr();

  if (existsPairs.includes(purePair) || purePair.length === 0) {
    return;
  }

  const worker = getNextWorker();
  const workerID = worker.id;
  const workerPairs = getPairsOfWorker(workerID);
  workerPairs.push(purePair);

  WORKER_REL_PAIR_OBJ[workerID] = workerPairs;
  PAIR_REL_WORKER_OBJ[purePair] = worker;
  VALS_OBJ[purePair] = [];
  WORKER_REL_PAIRS_COUNT[workerID] += 1;

  const finish = () => {
    worker.send({
      pair: purePair,
      action: common.ACTIONS.ADD,
    });
  };

  if (save) {
    existsPairs.push(purePair);
    savePairs(existsPairs).then(finish).catch(globPrintError);
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
  const worker = PAIR_REL_WORKER_OBJ[purePair];

  savePairs(existsPairs).then(() => {
    worker.send({
      pair: purePair,
      action: common.ACTIONS.REMOVE,
    });

    const workerID = worker.id;
    const workerPairs = getPairsOfWorker(workerID);
    const workerPairIdx = workerPairs.indexOf(purePair);

    if (workerPairIdx !== -1) {
      workerPairs.splice(workerPairIdx, 1);
    }

    WORKER_REL_PAIR_OBJ[workerID] = workerPairs;
    delete PAIR_REL_WORKER_OBJ[purePair];
    delete VALS_OBJ[purePair];
    WORKER_REL_PAIRS_COUNT[workerID] -= 1;
  }).catch(globPrintError);
};

const sendActionToAllWorkers = (data: ClusterMessage) => {
  const workerIDs = Object.keys(cluster.workers);
  const workersLength = workerIDs.length;
  let i = 0;

  for (; i < workersLength; i += 1) {
    const workerID = workerIDs[i];
    const worker = cluster.workers[workerID];
    worker.send(data);
  }
};

const pairActionConsumer = (function makePairActionConsumer() {
  const topic = 'pair-action';
  let consumer: ?Object = null;

  return Object.freeze({
    start() {
      this.stop();

      const newConsumer = new KafkaConsumer({
        'enable.auto.commit': true,
        'group.id': KAFKA_GROUP_ID,
        'metadata.broker.list': OPTIONS.KAFKA_BROKERS,
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
        'auto.commit.interval.ms': OPTIONS.STEP_DELAY,
        'enable.auto.commit': true,
        'group.id': KAFKA_GROUP_ID,
        'metadata.broker.list': OPTIONS.KAFKA_BROKERS,
      });

      newConsumer.on('ready', () => {
        newConsumer.subscribe([
          topic,
        ]);

        newConsumer.consume();
      });

      newConsumer.on('data', (data) => {
        const pairName = data.key;

        if (!has.call(VALS_OBJ, pairName)) {
          return;
        }

        const message = data.value.toString();
        const messParts = message.split(' ');
        VALS_OBJ[pairName].push(messParts);
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
  const dumpStep = 15000;
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
        data: VALS_OBJ,
      });

      VALS_OBJ = {};
      const pairsArr = getPairsArr();
      const pairsArrLength = pairsArr.length;
      let i = 0;

      for (; i < pairsArrLength; i += 1) {
        const pair = pairsArr[i];
        VALS_OBJ[pair] = [];
      }

      tick(OPTIONS.STEP_DELAY);
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
      const needTickDelay = OPTIONS.STEP_DELAY - (nowTime % OPTIONS.STEP_DELAY);
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
      VALS_OBJ = {};
    },
  });
}());

(function init() {
  const workerIDs = Object.keys(cluster.workers);
  const workersLength = workerIDs.length;
  let wI = 0;

  for (; wI < workersLength; wI += 1) {
    const workerID = workerIDs[wI];
    WORKER_REL_PAIRS_COUNT[workerID] = 0;
  }

  cluster.on('change', (worker: Object, newWorker: Object) => {
    const workerID = worker.id;
    const newWorkerID = newWorker.id;
    const workerPairs = getPairsOfWorker(workerID);
    const workerPairsLength = workerPairs.length;
    delete WORKER_REL_PAIRS_COUNT[workerID];
    delete WORKER_REL_PAIR_OBJ[workerID];
    WORKER_REL_PAIRS_COUNT[newWorkerID] = workerPairsLength;
    WORKER_REL_PAIR_OBJ[newWorkerID] = workerPairs;
    let pI = 0;

    for (; pI < workerPairsLength; pI += 1) {
      const pair = workerPairs[pI];
      PAIR_REL_WORKER_OBJ[pair] = newWorker;

      newWorker.send({
        pair,
        action: common.ACTIONS.ADD,
      });
    }
  });

  const initPromise = Promise.all([
    getPairs(),
    createPairFilesDir(),
  ]);

  let systemIsReady = false;
  let startWithPairs: ?Array<string> = null;

  const tryStart = () => {
    if (startWithPairs && systemIsReady) {
      const availablePairsLength = startWithPairs.length;
      let pI = 0;

      for (; pI < availablePairsLength; pI += 1) {
        const pair = startWithPairs[pI];
        addPair(pair);
      }

      messCron.start();
    }
  };

  initPromise.then(([pairs]) => {
    startWithPairs = pairs;
    tryStart();
  }).catch((error) => {
    throw error;
  });

  whenSystemReady(() => {
    systemIsReady = true;
    tryStart();
  });
}());
