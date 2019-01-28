// @flow
const cluster = require('cluster');
const fs = require('fs');
const path = require('path');

const dataDirName = 'data';
const envs = process.env;

global.IS_DEV_MODE = false;

global.globErrorHandler = () => {};

if (envs.NODE_ENV === 'development') {
  IS_DEV_MODE = true;

  globErrorHandler = (error: Error) => {
    throw error;
  };
}

global.IS_MASTER = cluster.isMaster;
global.ROOT_PATH = path.dirname(require.main.filename);
global.DATA_PATH = `${ROOT_PATH}/${dataDirName}`;

if (IS_MASTER) {
  try {
    const dataPathStat = fs.statSync(DATA_PATH);

    if (!dataPathStat.isDirectory()) {
      fs.mkdirSync(DATA_PATH);
    }
  } catch (error) {
    if (error.code !== 'ENOENT') {
      throw error;
    }

    fs.mkdirSync(DATA_PATH);
  }
}
