// @flow
const fs = require('fs');
const fastJSONStringify = require('fast-json-stringify');
const fastJSONParse = require('fast-json-parse');

const {
  KafkaConsumer,
  Producer,
} = require('node-rdkafka');

const common = require('./common');

const valsObjStringify = fastJSONStringify({
  type: 'array',
  items: {
    type: 'array',
    items: {
      type: 'number',
    },
  },
});

// Item structure: [pair, price, max, min, size, change, time]
type Value = number[];
type Values = Value[];
type Ticks = Array<string[]>;

// MARK: - Pair price & size consumer
const defAutoCommitDelayMs = 5000;
const delayFromOprionsMs = (OPTIONS.STEP_DELAY: number);
const AUTO_COMMIT_DELAY_MS = delayFromOprionsMs > defAutoCommitDelayMs ? delayFromOprionsMs : defAutoCommitDelayMs;
const KAFKA_GROUP_ID = `${common.NAME}-worker`;
const PAIR_PRICE_SIZE_TOPIC = 'pair-price-size';

KafkaConsumer.createReadStream({
  'auto.offset.reset': 'latest',
  'log.connection.close': false,
  'auto.commit.interval.ms': AUTO_COMMIT_DELAY_MS,
  'enable.auto.commit': true,
  'group.id': KAFKA_GROUP_ID,
  'bootstrap.servers': OPTIONS.KAFKA_BROKERS,
}, {}, {
  topics: [
    PAIR_PRICE_SIZE_TOPIC,
  ],
}).on('data', (mess) => {
  const pureMess = mess.value.toString();
  const messParts = pureMess.split(' ', 3);
  // eslint-disable-next-line flowtype-errors/show-errors
  process.send(messParts);
}).on('error', () => {});

// MARK: - Stats producer
const STATS_TOPIC_NAME = common.NAME;

const statsProducer = new Producer({
  acks: 0,
  dr_cb: false,
  dr_msg_cb: false,
  'log.connection.close': false,
  'bootstrap.servers': OPTIONS.KAFKA_BROKERS,
});

let sendMessage: Function = () => {};

statsProducer.on('ready', () => {
  sendMessage = (mess) => {
    const buffMess = Buffer.from(mess);

    try {
      statsProducer.produce(STATS_TOPIC_NAME, -1, buffMess);
      // eslint-disable-next-line no-empty
    } catch (error) {}
  };
}).on('disconnected', () => {
  sendMessage = () => {};
}).on('error', globThrowError);

statsProducer.connect();

const has = Object.prototype.hasOwnProperty;
const VALS_OBJ: { [string]: Values } = {};

const getFilePath = (pair: string) => `${common.PAIR_DUMPS_DIR_PATH}/${pair}.dmp`;

const pairExists = (pair: string) => has.call(VALS_OBJ, pair);

const getInitStatsForPair = (pair: string): Promise<Values> => {
  const filePath = getFilePath(pair);

  const getPromise = new Promise((resolve) => {
    fs.readFile(filePath, (error, data) => {
      let rArr: Values = [];

      if (data) {
        const { value } = fastJSONParse(data);

        if (value && Array.isArray(value)) {
          rArr = value;
        }
      }

      resolve(rArr);
    });
  });

  return getPromise;
};

const addPair = (pair: string) => {
  if (pairExists(pair)) {
    return;
  }

  getInitStatsForPair(pair).then((stats) => {
    VALS_OBJ[pair] = stats;
  }).catch(globThrowError);
};

const removePair = (pair: string) => {
  delete VALS_OBJ[pair];

  const filePath = getFilePath(pair);
  fs.unlink(filePath, (error) => {
    if (error) {
      globThrowError(error);
    }
  });
};

const savePair = (pair: string): Promise<void> => {
  if (!pairExists(pair)) {
    const saveError = new Error(`Pair "${pair}" not available`);
    return Promise.reject(saveError);
  }

  const savePath = getFilePath(pair);
  const saveDataStr = valsObjStringify(VALS_OBJ[pair]);

  const savePromise = new Promise((resolve, reject) => {
    fs.writeFile(savePath, saveDataStr, (error) => {
      if (error) {
        reject(error);
        return;
      }

      resolve();
    });
  });

  return savePromise;
};

const dumpHandler = (function makeDumpFunc() {
  let IS_DUMP_MODE = false;

  return () => {
    if (IS_DUMP_MODE) {
      return;
    }

    IS_DUMP_MODE = true;

    setImmediate(() => {
      const promisesArr: Promise<void>[] = [];
      const availablePairs = Object.keys(VALS_OBJ);
      const pLength = availablePairs.length;
      let i = 0;

      for (; i < pLength; i += 1) {
        const pair = availablePairs[i];
        const savePromise = savePair(pair);
        promisesArr[promisesArr.length] = savePromise;
      }

      Promise.all(promisesArr).then(() => {
        IS_DUMP_MODE = false;
      }).catch(() => {
        IS_DUMP_MODE = false;
      });
    });
  };
}());

const tickHandler = (data: { [string]: Ticks }) => {
  setImmediate(() => {
    const timeNow = Date.now();
    const availablePairs = Object.keys(VALS_OBJ);
    const pLength = availablePairs.length;
    const messages = {};
    let i = 0;

    for (; i < pLength; i += 1) {
      const pair = availablePairs[i];
      const pairDataArr = VALS_OBJ[pair];
      const addData: Ticks = Array.isArray(data[pair]) ? data[pair] : [];
      const addDataLength = addData.length;

      if (addDataLength > 0) {
        let addSize = 0;
        let addMax = 0;
        let addMin = Number.POSITIVE_INFINITY;
        let aI = 0;

        for (; aI < addDataLength; aI += 1) {
          const item = addData[aI];
          const price = +item[1];
          const size = +item[2];
          addSize += size;

          if (price > addMax) {
            addMax = price;
          }

          if (price < addMin) {
            addMin = price;
          }
        }

        const lastItem = addData[addDataLength - 1];
        const price = +lastItem[1];

        const addItem = [
          timeNow,
          price,
          addMax,
          addMin,
          addSize,
        ];

        pairDataArr.splice(0, 0, addItem);
      }

      let price = 0;
      let prevPrice = 0;
      let lastPrice = 0;
      let max = 0;
      let min = Number.POSITIVE_INFINITY;
      let size = 0;
      let tI = 0;
      const beforeTime = timeNow - OPTIONS.DURATION;
      const pairDataArrLength = pairDataArr.length;

      for (; tI < pairDataArrLength; tI += 1) {
        const pairDataItem = pairDataArr[tI];

        if (pairDataItem[0] < beforeTime) {
          if (tI > 0) {
            pairDataArr.splice(tI, pairDataArrLength);
          } else {
            min = 0;
          }

          break;
        }

        size += pairDataItem[4];

        if (pairDataItem[2] > max) {
          max = pairDataItem[2];
        }

        if (pairDataItem[3] < min) {
          min = pairDataItem[3];
        }
      }

      const pairDataArrNewLength = pairDataArr.length;

      if (pairDataArrNewLength > 0) {
        const fItem = pairDataArr[0];
        const sItem = pairDataArr[1];
        const lItem = pairDataArr[pairDataArrNewLength - 1];
        price = fItem[1];

        if (sItem) {
          prevPrice = sItem[1];
        } else {
          prevPrice = price;
        }

        lastPrice = lItem[1];
      } else {
        min = 0;
      }

      messages[pair] = `${price} ${prevPrice} ${lastPrice} ${max} ${min} ${size} ${timeNow}`;
    }

    const strMess = JSON.stringify(messages);
    sendMessage(strMess);
  });
};

const processActions = {
  [common.ACTION.TICK](tick: Object) {
    tickHandler(tick.data);
  },

  [common.ACTION.DUMP]: dumpHandler,

  [common.ACTION.ADD_PAIR](data: Object) {
    addPair(data.pair);
  },

  [common.ACTION.REMOVE_PAIR](data: Object) {
    removePair(data.pair);
  },
};

process.on('message', (data: ClusterMessage) => {
  const actionHandler = processActions[data.action];

  if (typeof actionHandler === 'function') {
    actionHandler(data);
  }
});
