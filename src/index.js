// @flow
const cluster = require('cluster');
require('./config');

if (IS_MASTER) {
  // eslint-disable-next-line global-require
  const os = require('os');

  const appErrorHandler = (error) => {
    if (IS_DEV_MODE) {
      // eslint-disable-next-line no-console
      console.log(`🐞  \x1b[31m[APP ERROR] ${error.message}\x1b[37m`);
      process.exit(0);
      return;
    }

    process.abort();
  };

  process.on('uncaughtException', appErrorHandler);
  process.on('unhandledRejection', appErrorHandler);

  const cpusLength = os.cpus().length;

  for (let i = 0; i < cpusLength; i += 1) {
    cluster.fork();
  }
}

require('./startup');

/*
const kafka = require('node-rdkafka');

const {
  KAFKA_BROKER_LIST,
} = process.env;

if (!KAFKA_BROKER_LIST) {
  throw new Error('Attribute "KAFKA_BROKER_LIST" is required.Please set');
}

console.log(kafka.librdkafkaVersion);
*/
