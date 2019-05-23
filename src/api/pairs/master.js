// @flow
const cluster = require('cluster');
const { KafkaConsumer } = require('node-rdkafka');
const fs = require('fs');
const common = require('./common');

const has = Object.prototype.hasOwnProperty;
const KAFKA_GROUP_ID = `${common.NAME}-master`;
const PAIR_REL_WORKER_OBJ: { [string]: Object } = {};
const WORKER_REL_PAIR_OBJ: { [string]: string[] } = {};
const WORKER_REL_PAIRS_COUNT: { [string]: number } = {};
let VALS_OBJ: { [string]: Array<string[]> } = {};

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
  const readFile = (callback: Function) => {
    fs.readFile(common.PAIRS_FILE_PATH, 'utf8', callback);
  };

  const strAsPairsArr = (str: string) => {
    const pairArr = str.split('\n');
    const pairArrLength = pairArr.length;
    const rArr = [];
    let i = 0;

    for (; i < pairArrLength; i += 1) {
      const pair = pairArr[i];
      const purePair = pair.trim();
      rArr[rArr.length] = purePair;
    }

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

const getPairsOfWorker = (wID: string) => {
  let returnArr: string[] = [];
  const pairsOfWorker = WORKER_REL_PAIR_OBJ[wID];

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

const getExistsPairs = () => Object.keys(PAIR_REL_WORKER_OBJ);

const getPurePair = (pair: any) => (typeof pair === 'string' ? pair.trim() : '');

const isExistsPair = (pair: string) => has.call(PAIR_REL_WORKER_OBJ, pair);

const synchPairs = (): Promise<void> => {
  const existsPairs = getExistsPairs();

  const savePromise = new Promise((resolve, reject) => {
    fs.writeFile(common.PAIRS_FILE_PATH, existsPairs.join('\n'), (error) => {
      if (error) {
        reject(error);
        return;
      }

      resolve();
    });
  });

  return savePromise;
};

const addPair = (pair: string): boolean => {
  if (isExistsPair(pair) || pair.length === 0) {
    return false;
  }

  const worker = getNextWorker();
  const workerID = worker.id;
  const workerPairs = getPairsOfWorker(workerID);
  workerPairs[workerPairs.length] = pair;

  WORKER_REL_PAIR_OBJ[workerID] = workerPairs;
  PAIR_REL_WORKER_OBJ[pair] = worker;
  VALS_OBJ[pair] = [];
  WORKER_REL_PAIRS_COUNT[workerID] += 1;

  worker.send({
    pair,
    action: common.ACTION.ADD_PAIR,
  });

  return true;
};

const removePair = (pair: string): boolean => {
  if (!isExistsPair(pair) || pair.length === 0) {
    return false;
  }

  const worker = PAIR_REL_WORKER_OBJ[pair];

  worker.send({
    pair,
    action: common.ACTION.REMOVE_PAIR,
  });

  const workerID = worker.id;
  const workerPairs = getPairsOfWorker(workerID);
  const workerPairIdx = workerPairs.indexOf(pair);

  if (workerPairIdx !== -1) {
    workerPairs.splice(workerPairIdx, 1);
  }

  WORKER_REL_PAIR_OBJ[workerID] = workerPairs;
  delete PAIR_REL_WORKER_OBJ[pair];
  delete VALS_OBJ[pair];
  WORKER_REL_PAIRS_COUNT[workerID] -= 1;
  return true;
};

const makeActionWithWorker = (func: Function) => {
  const workerIDs = Object.keys(cluster.workers);
  const workersLength = workerIDs.length;
  let i = 0;

  for (; i < workersLength; i += 1) {
    const workerID = workerIDs[i];
    const worker = cluster.workers[workerID];
    func(worker);
  }
};

const actionsConsumer = (function makeActionsConsumer() {
  const topic = `${common.NAME}-action`;
  let consumer: ?Object = null;

  const pairActions = {
    [common.ACTION.ADD_PAIR](pair: string) {
      if (addPair(pair)) {
        synchPairs().catch(globThrowError);
      }
    },

    [common.ACTION.REMOVE_PAIR](pair: string) {
      if (removePair(pair)) {
        synchPairs().catch(globThrowError);
      }
    },
  };

  return Object.freeze({
    start() {
      this.stop();

      const newConsumer = new KafkaConsumer({
        'bootstrap.servers': OPTIONS.KAFKA_BROKERS,
        'enable.auto.commit': true,
        'group.id': KAFKA_GROUP_ID,
        'log.connection.close': false,
      });

      newConsumer.on('ready', () => {
        newConsumer.subscribe([
          topic,
        ]);

        newConsumer.consume();
      });

      newConsumer.on('data', (data) => {
        const mess = data.value.toString();
        let obj: ?Object = null;

        try {
          obj = JSON.parse(mess);
          // eslint-disable-next-line no-empty
        } catch (error) {}

        if (obj && typeof obj.action === 'string') {
          const pureAction = obj.action.trim().toUpperCase();
          const action = pairActions[pureAction];

          if (typeof action === 'function') {
            const purePair = getPurePair(obj.pair);
            action(purePair);
          }
        }
      });

      newConsumer.on('error', () => {});
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

const ticksHandler = (worker: Object, tick: any[]) => {
  const pairName = tick[0];

  if (!isExistsPair(pairName)) {
    if (!addPair(pairName)) {
      return;
    }

    synchPairs();
  }

  const pairData = VALS_OBJ[pairName];
  pairData[pairData.length] = tick;
};

const messCron = (function makeMessCron() {
  const dumpStep = 30000;
  let dumpTimeoutID: ?TimeoutID = null;
  let tickTimeoutID: ?TimeoutID = null;

  const dump = (msDelay: number) => {
    dumpTimeoutID = setTimeout(() => {
      makeActionWithWorker((worker) => {
        worker.send({
          action: common.ACTION.DUMP,
        });
      });

      dump(dumpStep);
    }, msDelay);
  };

  const resetData = () => {
    VALS_OBJ = {};
    const pairsArr = getExistsPairs();
    const pairsArrLength = pairsArr.length;
    let i = 0;

    for (; i < pairsArrLength; i += 1) {
      const pair = pairsArr[i];
      VALS_OBJ[pair] = [];
    }
  };

  const tick = (msDelay: number) => {
    tickTimeoutID = setTimeout(() => {
      makeActionWithWorker((worker) => {
        worker.send({
          action: common.ACTION.TICK,
          data: VALS_OBJ,
        });
      });

      resetData();
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
      actionsConsumer.start();
      cluster.on('message', ticksHandler);
    },

    stop() {
      stopTimeoutIfNeeded(dumpTimeoutID);
      stopTimeoutIfNeeded(tickTimeoutID);
      actionsConsumer.stop();
      cluster.off('message', ticksHandler);
      resetData();
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
        action: common.ACTION.ADD_PAIR,
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
