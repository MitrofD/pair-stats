// @flow
const cluster = require('cluster');
const fs = require('fs');
const common = require('./common');

(function makeDirIfNeeded() {
  try {
    const pairsPathStat = fs.statSync(common.FILES_PATH);

    if (!pairsPathStat.isDirectory()) {
      fs.mkdirSync(common.FILES_PATH);
    }
  } catch (error) {
    if (error.code !== 'ENOENT') {
      throw error;
    }

    fs.mkdirSync(common.FILES_PATH);
  }
}());

const startWorkersLength = Object.keys(cluster.workers).length;

if (startWorkersLength === 0) {
  throw new Error('Not found workers');
}

const WORKERS_ARR: Object[] = Object.values(cluster.workers);
const PAIRS_REL_WORKER: { [string]: Object } = {};

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

const getPairFileNames = (): Promise<string[]> => {
  const getPromise = new Promise((resolve, reject) => {
    fs.readdir(common.FILES_PATH, {
      encoding: 'utf8',
      withFileTypes: true,
    }, (error, files) => {
      if (error) {
        reject(error);
        return;
      }

      const pureFiles: string[] = [];

      /* eslint-disable flowtype-errors/show-errors */
      files.forEach((file) => {
        if (file.isFile()) {
          pureFiles.push(file.name);
        }
      });
      /* eslint-enable flowtype-errors/show-errors */

      resolve(pureFiles);
    });
  });

  return getPromise;
};

const addPair = (pair: string) => {
  const worker = getNextWorker();

  worker.send({
    pair,
    action: common.ACTIONS.ADD_PAIR,
  });
};

const sendActionToAllWorkers = (data: any) => {
  for (let i = 0; i < WORKERS_ARR.length; i += 1) {
    const worker = WORKERS_ARR[i];
    worker.send(data);
  }
};

const allWorkersDidInit = () => {
  getPairFileNames().then((files) => {
    files.forEach(addPair);
  }).catch((error) => {
    throw error;
  });
};

(function applyWorkers() {
  let currCount = 0;
  const onlineEventName = 'online';

  function onlineFunc() {
    this.off(onlineEventName, onlineFunc);
    currCount += 1;

    if (currCount === startWorkersLength) {
      allWorkersDidInit();
    }
  }

  const workerKeys = Object.keys(cluster.workers);
  workerKeys.forEach((workerId) => {
    const worker = cluster.workers[workerId];
    worker.on(onlineEventName, onlineFunc);
  });
}());
