// @flow
const path = require('path');

const envs = process.env;

global.IS_DEV_MODE = false;

global.globThrowError = () => {};

if (envs.NODE_ENV === 'development') {
  IS_DEV_MODE = true;
  globThrowError = showError;
}

global.ROOT_PATH = path.dirname(require.main.filename);
