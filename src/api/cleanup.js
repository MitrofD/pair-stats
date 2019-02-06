// @flow
const has = Object.prototype.hasOwnProperty;

const subs: { [string]: Function } = {};
let isCalled = false;

const getUniqueName = (function makeUniqueName() {
  const basicPart = 'clnp_sub';
  let idx = 0;

  return () => {
    idx += 1;
    return `${basicPart}_${idx}`;
  };
}());

function exit(callExit: boolean, signal: number) {
  if (isCalled) {
    return;
  }

  isCalled = true;
  const subKeys = Object.keys(subs);
  const totalSignal = 128 + signal;

  subKeys.forEach((key) => {
    const sub = subs[key];
    sub(callExit, totalSignal);
  });
}

process.on('exit', exit);
process.on('SIGINT', exit.bind(null, true, 2));
process.on('SIGTERM', exit.bind(null, true, 15));

const subscribe = (func: Function) => {
  const uniqueName = getUniqueName();
  subs[uniqueName] = func;
  return uniqueName;
};

const unsubscribe = (subName: string) => {
  if (has.call(subs, subName)) {
    delete subs[subName];
    return true;
  }

  return false;
};

module.exports = {
  subscribe,
  unsubscribe,
};
