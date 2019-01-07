// @flow
const cluster = require('cluster');
const fs = require('fs');
const common = require('./common');
const tools = require('../tools');

const actions = {
  [common.ACTIONS.ADD_PAIR](data: Object) {
    const type = typeof data;
    const strData = JSON.stringify(data);
    console.log(cluster.worker.id, type, strData);
  },
};

process.on('message', (data) => {
  if (tools.has.call(actions, data.action)) {
    actions[data.action](data);
  }

  console.log(data.action);
});

/*
const common = require('./common');
const fs = require('fs');

const fileExt = 'txt';
const getFilePath = (pairName) => `${common.FILES_PATH}/${pairName}.${fileExt}`;

const addPair = (pairName) => {
  const filePath = getFilePath(pairName);

  const filePromise = new Promise((resolve, reject) => {
    fs.open(filePath, 'a', (err, fd) => {
      if (err) {
        reject(err);
        return;
      }

      resolve({
        fd,
        pair: pairName,
      });
    });
  });

  return filePromise;
};

const tmpPairNames = [
  'BTC_USDT',
  'ETH_USDT',
  'LTC_USDT',
  'NEO_USDT',
  'EOS_USDT',
  'EOS_ETH',
  'ICX_ETH',
  'LTC_ETH',
  'NEO_ETH',
  'EOS_BTC',
  'ICX_BTC',
  'LTC_BTC',
  'NEO_BTC',
  'USDT_BTC',
  'ETH_BTC',
];

const avgPairsCountForCluster = Math.ceil(tmpPairNames.length / WORKERS_LENGTH);
const startIdx = WORKER_NUM * avgPairsCountForCluster;
const pairNamesForCluster = tmpPairNames.slice(startIdx, startIdx + avgPairsCountForCluster);

const PAIRS_DATA = {};

pairNamesForCluster.forEach((pairName) => {
  addPair(pairName).then((pairData) => {
    PAIRS_DATA[pairData.pair] = pairData;
  }).catch((error) => {
    throw error;
  });
});

const pairs = {
  available: () => pairNamesForCluster.slice(),
};

module.exports = pairs;
*/
