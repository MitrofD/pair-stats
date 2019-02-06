// @flow
const cluster = require('cluster');

global.IS_MASTER = cluster.isMaster;

/* eslint-disable no-console */
global.showError = (error: Object) => {
  console.log('\x1b[31m');
  console.log(error);
  console.log('\x1b[37m');
};

global.showSuccessMessage = (mess: string) => {
  console.log(`\x1b[32m${mess}\x1b[37m`);
};
/* eslint-enable no-console */

const whenSystemReady = (func: Function) => {
  func();
};

if (IS_MASTER) {
  const appErrorHandler = (error) => {
    showError(error);
    process.exit(1);
  };

  process.on('uncaughtException', appErrorHandler);
  process.on('unhandledRejection', appErrorHandler);

  const readySubs: Function[] = [];

  global.whenSystemReady = (func: Function) => {
    readySubs.push(func);
  };

  /* eslint-disable global-require */
  const os = require('os');
  require('./config');
  /* eslint-enable global-require */

  const {
    DURATION_MS,
    KAFKA_BROKERS,
    STEP_DELAY_MS,
  } = process.env;

  const kafkaBrokersList = (function getPureKafkaBrokers() {
    const emptyStr = '';
    const brokerList = typeof KAFKA_BROKERS === 'string' ? KAFKA_BROKERS.replace(' ', emptyStr) : emptyStr;
    const brokerListArr = brokerList.split(',');
    const brokerListLength = brokerListArr.length;

    if (brokerList.length === 0) {
      throw new Error('Settings option "KAFKA_BROKERS" is required');
    }

    const urlRegExp = /^(https?:\/\/)?(?:[^@/\n]+@)?(?:www\.)?([^:/\n]+)((?::\d+)?)$/i;
    let i = 0;

    for (; i < brokerListLength; i += 1) {
      const broker = brokerListArr[i];

      if (!urlRegExp.test(broker)) {
        throw new Error('Settings option "KAFKA_BROKERS" is incorrect.(Ex: 127.0.0.1:9092,127.0.0.1:9093)');
      }
    }

    return brokerList;
  }());

  const pureDuration = parseInt(DURATION_MS) || 86400000;
  const pureStepDelay = parseInt(STEP_DELAY_MS) || 1000;

  if (pureDuration < pureStepDelay) {
    throw new Error('Statistics step delay can\'t be great than duration');
  }

  global.OPTIONS = {
    DURATION: pureDuration,
    KAFKA_BROKERS: kafkaBrokersList,
    STEP_DELAY: pureStepDelay,
  };

  const clusterEnvs = {
    OPTIONS: JSON.stringify(OPTIONS),
  };

  const workersLength = os.cpus().length;
  const emptyOrS = workersLength > 1 ? 's' : '';
  let loadedWorkers = 0;

  showSuccessMessage(`🤴🏼 Master wait ${workersLength} worker${emptyOrS}:\n`);

  const workerIsReady = (readyWorker: Object) => {
    loadedWorkers += 1;
    showSuccessMessage(`👷 Worker "${readyWorker.id}" is ready`);

    if (loadedWorkers === workersLength) {
      global.whenSystemReady = whenSystemReady;
      showSuccessMessage('\n🏄 Let\'s go!');

      readySubs.forEach((sub) => {
        sub();
      });

      // eslint-disable-next-line flowtype-errors/show-errors
      cluster.off('online', workerIsReady);

      const workerFail = (function makeWorkerFail() {
        const resetAfterMS = workersLength * pureStepDelay;
        let length = 0;

        return (errorCode: number) => {
          length += 1;

          if (length === workersLength) {
            throw new Error(`Worcer Error.Code ${errorCode}`);
          }

          setTimeout(() => {
            length = 0;
          }, resetAfterMS);
        };
      }());

      cluster.on('exit', (worker: Object, code: number) => {
        if (!worker.exitedAfterDisconnect) {
          const newWorker = cluster.fork(clusterEnvs);
          workerFail(code);

          newWorker.once('online', () => {
            cluster.emit('change', worker, newWorker);
          });
        }
      });
    }
  };

  cluster.on('online', workerIsReady);

  let i = 0;

  for (; i < workersLength; i += 1) {
    cluster.fork(clusterEnvs);
  }
} else {
  // eslint-disable-next-line global-require
  require('./config');

  // eslint-disable-next-line flowtype-errors/show-errors
  global.OPTIONS = JSON.parse(process.env.OPTIONS);
  global.whenSystemReady = whenSystemReady;
}

require('./startup');
