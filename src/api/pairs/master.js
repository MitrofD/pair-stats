// @flow
const cluster = require('cluster');
const { KafkaConsumer } = require('node-rdkafka');
const fs = require('fs');
const common = require('./common');
const tools = require('../tools');

type ProcessAction = Object & {
  action: string,
};

const WORKERS_ARR: Object[] = Object.values(cluster.workers);
const PAIRS_REL_WORKER: { [string]: Object } = {};
let TMP_DATA: { [string]: string[] } = {};

const startWorkersLength = Object.keys(cluster.workers).length;

if (startWorkersLength === 0) {
  throw new Error('Not found workers');
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

const addPair = (pair: string) => {
  const worker = getNextWorker();
  PAIRS_REL_WORKER[pair] = worker;

  worker.send({
    pair,
    action: common.ACTIONS.ADD_PAIR,
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

const kafkaConsumer = (function makeKafkaConsumer() {
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

  const commitCount = 5000;
  const topicName = 'pair-price-size';
  let consumer: ?Object = null;
  let messCounter = 0;

  return Object.freeze({
    start() {
      this.stop();

      const newConsumer = new KafkaConsumer({
        'enable.auto.commit': false,
        'group.id': 'pair-stats',
        'metadata.broker.list': brokerList,
      });

      newConsumer.on('ready', () => {
        newConsumer.subscribe([
          topicName,
        ]);

        newConsumer.consume();
      });

      newConsumer.on('data', (data) => {
        const totalMess = data.value.toString();
        const messages = totalMess.split(',');
        const messagesLength = messages.length;
        let iM = 0;

        for (iM; iM < messagesLength; iM += 1) {
          messCounter += 1;

          if (messCounter === commitCount) {
            newConsumer.commit(data);
            messCounter = 0;
          }

          const mess = messages[iM];
          const messParts = mess.split(' ');
          const pairName = messParts[0];
          const item = [
            messParts[1],
            messParts[2],
          ];

          if (!tools.has.call(TMP_DATA, pairName)) {
            TMP_DATA[pairName] = [item];
            return;
          }

          TMP_DATA[pairName].push(item);
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
        data: TMP_DATA,
      });

      TMP_DATA = {};
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
      kafkaConsumer.start();
    },

    stop() {
      stopTimeoutIfNeeded(dumpTimeoutID);
      stopTimeoutIfNeeded(tickTimeoutID);
      kafkaConsumer.stop();
      TMP_DATA = {};
    },
  });
}());

let initPairs: Array<string> = [];
let workersDidInit = false;
let fileStructDidInit = false;

const startWorkersIfNeeded = () => {
  if (workersDidInit && fileStructDidInit) {
    initPairs.forEach(addPair);
    messCron.start();
  }
};

createFileAndDirIdNeeded.then(([pairs]) => {
  fileStructDidInit = true;
  initPairs = pairs;
  startWorkersIfNeeded();
}).catch((error) => {
  throw error;
});

(function initWorkers() {
  let currCount = 0;
  const eventName = 'online';

  function onlineFunc() {
    this.off(eventName, onlineFunc);
    currCount += 1;

    if (currCount === startWorkersLength) {
      workersDidInit = true;
      startWorkersIfNeeded();
    }
  }

  let i = 0;
  const workersLength = WORKERS_ARR.length;

  for (i; i < workersLength; i += 1) {
    const worker = WORKERS_ARR[i];
    worker.on(eventName, onlineFunc);
  }
}());
