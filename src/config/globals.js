// @flow
const path = require('path');

const envs = process.env;

global.IS_DEV_MODE = false;

global.globPrintError = () => {};

if (envs.NODE_ENV === 'development') {
  IS_DEV_MODE = true;

  globPrintError = (error: Error) => {
    throw error;
  };
}

global.ROOT_PATH = path.dirname(require.main.filename);
