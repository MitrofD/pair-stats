// @flow
/* eslint-disable global-require */
if (IS_MASTER) {
  require('./master');
} else {
  require('./worker');
}
/* eslint-enable global-require */
