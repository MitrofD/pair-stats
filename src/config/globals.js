// @flow
const path = require('path');

global.IS_DEV_MODE = false;
global.globThrowError = () => {};

if (process.env.NODE_ENV === 'development') {
  IS_DEV_MODE = true;
  globThrowError = showError;
}

global.ROOT_PATH = path.dirname(require.main.filename);
