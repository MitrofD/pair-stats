// @flow
const fs = require('fs');
const fastJsonStringify = require('fast-json-stringify');

const {
  KafkaConsumer,
  Producer,
} = require('node-rdkafka');

const common = require('./common');

const ticksStringify = fastJsonStringify({
  type: 'array',
  items: {
    type: 'array',
    items: {
      type: 'number',
    },
  },
});

// Item structure: [time, price, max, min, size]
type Value = number[];
type Values = Value[];
type Ticks = Array<string[]>;

// MARK: - Pair price & size consumer
const defAutoCommitDelayMs = 5000;
let AUTO_COMMIT_DELAY_MS = (OPTIONS.STEP_DELAY: number);

if (AUTO_COMMIT_DELAY_MS < defAutoCommitDelayMs) {
  AUTO_COMMIT_DELAY_MS = defAutoCommitDelayMs;
}

const KAFKA_GROUP_ID = `${common.NAME}-worker`;
const PAIR_PRICE_SIZE_TOPIC = 'pair-price-size';
let PREV_TICKS = [];

const pairPriceSizeConsumer = new KafkaConsumer({
  'auto.commit.interval.ms': AUTO_COMMIT_DELAY_MS,
  'enable.auto.commit': true,
  'group.id': KAFKA_GROUP_ID,
  'bootstrap.servers': OPTIONS.KAFKA_BROKERS,
});

pairPriceSizeConsumer.on('ready', () => {
  pairPriceSizeConsumer.subscribe([
    PAIR_PRICE_SIZE_TOPIC,
  ]);

  pairPriceSizeConsumer.consume();
});

pairPriceSizeConsumer.on('data', (data) => {
  const mess = data.value.toString();
  const messParts = mess.split(' ', 3);
  // eslint-disable-next-line flowtype-errors/show-errors
  process.send(messParts);
});

pairPriceSizeConsumer.connect();

// MARK: - Stats producer
const STATS_TOPIC_NAME = common.NAME;

const statsProducer = new Producer({
  acks: 0,
  dr_cb: false,
  dr_msg_cb: false,
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
});

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
        try {
          // eslint-disable-next-line flowtype-errors/show-errors
          const json = JSON.parse(data);

          if (Array.isArray(json)) {
            rArr = json;
          }
          // eslint-disable-next-line no-empty
        } catch (parseError) {}
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
  const saveDataStr = ticksStringify(VALS_OBJ[pair]);

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
        promisesArr.push(savePromise);
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
  const timeNow = Date.now();
  const availablePairs = Object.keys(VALS_OBJ);
  const pLength = availablePairs.length;
  let i = 0;

  PREV_TICKS = [];

  for (; i < pLength; i += 1) {
    const pair = availablePairs[i];
    const pairDataArr = VALS_OBJ[pair];

    // eslint-disable-next-line no-loop-func
    setImmediate(() => {
      const addData: Ticks = Array.isArray(data[pair]) ? data[pair] : [];
      const addDataLength = addData.length;

      if (addDataLength > 0) {
        let addSize = 0;
        let addMax = 0;
        let addMin = Number.POSITIVE_INFINITY;
        let aI = 0;

        for (; aI < addDataLength; aI += 1) {
          const addDataItem = addData[aI];
          const price = +addDataItem[0];
          const size = +addDataItem[1];
          addSize += size;

          if (price > addMax) {
            addMax = price;
          }

          if (price < addMin) {
            addMin = price;
          }
        }

        const lastItem = addData[addDataLength - 1];
        const price = +lastItem[0];

        const addItem = [
          timeNow,
          price,
          addMax,
          addMin,
          addSize,
        ];

        pairDataArr.splice(0, 0, addItem);
      }

      const beforeTime = timeNow - OPTIONS.DURATION;
      const pairDataArrLength = pairDataArr.length;

      let price = 0;
      let max = 0;
      let min = Number.POSITIVE_INFINITY;
      let size = 0;
      let tI = 0;

      for (; tI < pairDataArrLength; tI += 1) {
        const pairDataItem = pairDataArr[tI];

        if (pairDataItem[0] < beforeTime) {
          const sliceLength = pairDataArrLength - tI;
          pairDataArr.splice(tI, sliceLength);
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
      let change = 0;

      if (pairDataArrNewLength > 0) {
        const fItem = pairDataArr[0];
        const lItem = pairDataArr[pairDataArrNewLength - 1];
        const lastPrice = lItem[1];
        price = fItem[1];
        const diff = price - lastPrice;
        change = diff / lastPrice * 100;
      } else {
        min = 0;
      }

      const message = `${pair} ${price} ${max} ${min} ${size} ${change} ${timeNow}`;
      PREV_TICKS.push(message);
      sendMessage(message);
    });
  }
};

const processActions = {
  [common.ACTION.TICK](tick: Object) {
    tickHandler(tick.data);
  },

  [common.ACTION.DUMP]: dumpHandler,

  [common.ACTION.FORCE_TICK]() {
    const ticksLength = PREV_TICKS.length;
    let i = 0;

    for (; i < ticksLength; i += 1) {
      const tickMessage = PREV_TICKS[i];
      sendMessage(tickMessage);
    }
  },

  [common.ACTION.ADD](data: Object) {
    addPair(data.pair);
  },

  [common.ACTION.REMOVE](data: Object) {
    removePair(data.pair);
  },
};

process.on('message', (data: ClusterMessage) => {
  const actionHandler = processActions[data.action];

  if (typeof actionHandler === 'function') {
    actionHandler(data);
  }
});
