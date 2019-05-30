// @flow
global.IS_DEV_MODE = false;
global.globThrowError = showError; // () => {};

if (process.env.NODE_ENV === 'development') {
  IS_DEV_MODE = true;
  globThrowError = showError;
}
