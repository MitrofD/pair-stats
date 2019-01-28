// @flow
const cluster = require('cluster');
require('./config');

const appErrorHandler = (error) => {
  if (IS_DEV_MODE) {
    console.log(error);
    // eslint-disable-next-line no-console
    // console.log(`🐞  \x1b[31m[APP ERROR] ${error.message}\x1b[37m`);
    process.exit(0);
    return;
  }

  process.abort();
};

process.on('uncaughtException', appErrorHandler);
process.on('unhandledRejection', appErrorHandler);

if (IS_MASTER) {
  // eslint-disable-next-line global-require
  const os = require('os');
  const cpusLength = os.cpus().length;
  let i = 0;

  for (i; i < cpusLength; i += 1) {
    cluster.fork();
  }
}

require('./startup');
