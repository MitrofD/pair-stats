// @flow
const cluster = require('cluster');
const { KafkaConsumer } = require('node-rdkafka');
const fs = require('fs');
const common = require('./common');
const tools = require('../tools');

type ProcessAction = Object & {
  action: string,
};

let DATA: { [string]: string[] } = {};
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

const getPairsPromise = getPairs();
const createPairFilesDirPromise = createPairFilesDir();
const createFileAndDirIdNeeded = Promise.all([
  getPairsPromise,
  createPairFilesDirPromise,
]);

/*
const workers = (function makeWorker() {
  const resetIdx = 0;
  const tmpWorkers: Object[] = [];
  let currIdx = resetIdx;

  return Object.freeze({
    get() {
      if (tmpWorkers.length > 0) {
        return tmpWorkers.shift();
      }

      let worker = WORKERS_ARR[currIdx];

      if (!worker) {
        currIdx = resetIdx;
        worker = WORKERS_ARR[resetIdx];
      }

      currIdx += 1;
      return worker;
    },

    toTMP(worker: Object) {
      tmpWorkers.push(worker);
    },
  });
}());
*/

const getNextWorker = () => {
  const workerIds = Object.keys(cluster.workers);
  const workerLength = workerIds.length;

  let minCount = Number.POSITIVE_INFINITY;
  let id = workerIds[0];
  let i = 0;

  for (i; i < workerLength; i += 1) {
    const workerId = workerIds[i];

    if (!tools.has.call(WORKER_REL_PAIRS_COUNT, workerId)) {
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

/*
const getNextWorker = (function genGetNextWorker() {
  const resetIdx = 0;
  let currWorkerIdx = resetIdx;

  return (): Object => {
    let worker = WORKERS_ARR[currWorkerIdx];

    if (!worker) {
      currWorkerIdx = resetIdx;
      worker = WORKERS_ARR[resetIdx];
    }

    currWorkerIdx += 1;
    return worker;
  };
}());
*/

const addPair = (pair: string) => {
  const worker = getNextWorker();
  PAIRS_REL_WORKER[pair] = worker;
  WORKER_REL_PAIRS_COUNT[pair] += 1;

  worker.send({
    pair,
    action: common.ACTIONS.ADD_PAIR,
  });
};

const removePair = (pair: string) => {
  if (!tools.has.call(PAIRS_REL_WORKER, pair)) {
    return;
  }

  const worker = PAIRS_REL_WORKER[pair];
  delete PAIRS_REL_WORKER[pair];
  WORKER_REL_PAIRS_COUNT[pair] -= 1;

  worker.send({
    pair,
    action: common.ACTIONS.REMOVE_PAIR,
  });
};

const sendActionToAllWorkers = (data: ProcessAction) => {
  let i = 0;
  const workersLength = WORKERS_ARR.length;

  for (i; i < workersLength; i += 1) {
    const worker = WORKERS_ARR[i];
    worker.send(data);
  }
};

const KAFKA_BROKERS_LIST = (function getPureKfkaBrokers() {
  const {
    KAFKA_BROKERS,
  } = process.env;

  const emptyStr = '';
  const brokerList = typeof KAFKA_BROKERS === 'string' ? KAFKA_BROKERS.replace(' ', emptyStr) : emptyStr;
  const brokerListArr = brokerList.split(',');
  const brokerListLength = brokerListArr.length;

  if (brokerListLength === 0) {
    throw new Error('Settings option "KAFKA_BROKERS" is required');
  }

  let i = 0;

  for (i; i < brokerListLength; i += 1) {
    const broker = brokerListArr[i];

    if (!tools.urlRegExp.test(broker)) {
      throw new Error('Settings option "KAFKA_BROKERS" is incorrect.(Ex: 127.0.0.1:9092,127.0.0.1:9093)');
    }
  }

  return brokerList;
}());
const KAFKA_GROUP_ID = 'pair-stats';

const pairActionConsumer = (function makePairActionConsumer() {
  const topic = 'pair-action';
  let consumer: ?Object = null;

  return Object.freeze({
    start() {
      this.stop();

      const newConsumer = new KafkaConsumer({
        'enable.auto.commit': false,
        'group.id': KAFKA_GROUP_ID,
        'metadata.broker.list': KAFKA_BROKERS_LIST,
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
        const pairName = data.key;

        if (pureAction === common.ACTIONS.ADD_PAIR) {
          addPair(pairName);
          return;
        }

        if (pureAction === common.ACTIONS.REMOVE_PAIR) {
          removePair(pairName);
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
        'metadata.broker.list': KAFKA_BROKERS_LIST,
      });

      newConsumer.on('ready', () => {
        newConsumer.subscribe([
          topic,
        ]);

        newConsumer.consume();
      });

      newConsumer.on('data', (data) => {
        const message = data.value.toString();
        const messParts = message.split(' ');
        const pairName = data.key;

        if (!tools.has.call(DATA, pairName)) {
          DATA[pairName] = [messParts];
          return;
        }

        DATA[pairName].push(messParts);
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
  const dumpStep = 30000;
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
        data: DATA,
      });

      DATA = {};
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
      DATA = {};
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
