const { Kafka } = require('kafkajs');
const { SchemaRegistry } = require('@kafkajs/confluent-schema-registry');

// Kafka configuration
const kafkaConfig = {
  clientId: 'uk-capacity-consumer',
  brokers: ['pkc-56d1g.eastus.azure.confluent.cloud:9092'],
  ssl: true,
  sasl: {
    mechanism: 'plain',
    username: 'SCKEWIJPMXFNC5NF',
    password: 'f6PC/W/nAyXEC3hU4FJYfpf6zZwrDqewUKN/OaOIctTX6bGOpBljlfkOseo9t4Mw'
  },
  consumer: {
    'auto.offset.reset': 'latest',
    'enable.auto.commit': true,
    'auto.commit.interval.ms': 1000
  }
};

const schemaRegistryConfig = {
  host: 'https://psrc-4r0k9.westus2.azure.confluent.cloud',
  auth: {
    username: '2XMWBL5YR6OB4YG2', 
    password: '+srHchih77Hlq77xkmmMoU5OkRCCKZmMBSnbjcXVyuZEsjSoJae7PNNnGIpdV3of'
  },
};

const registry = new SchemaRegistry(schemaRegistryConfig);
const kafka = new Kafka(kafkaConfig);
const consumer = kafka.consumer({ groupId: 'uk-capacity-consumer' });

const dataObj = {};
const ukArr = [
  1183, 1186, 1187, 1188, 1189, 1191, 1192, 1193, 1225, 1238, 1249, 1253, 1254,
  1291, 1292, 1294, 1301, 1302, 1308, 1309, 1317, 1318, 1319, 1321, 1323, 1352,
  1353, 1368, 1369, 1375, 1380, 1381, 1383, 1391, 1398, 1537
];

let isProcessing = false;
const messageQueue = []; // Holds messages for processing

const isTimestampInWorkingDay = (timestamp) => {
  const eventDate = new Date(timestamp);
  const currentDate = new Date();
  let startOfWorkingDay = new Date(currentDate);
  let endOfWorkingDay = new Date(currentDate);

  if (currentDate.getHours() < 3) {
    startOfWorkingDay.setDate(currentDate.getDate() - 1);
  } else {
    endOfWorkingDay.setDate(currentDate.getDate() + 1);
  }

  startOfWorkingDay.setHours(6, 0, 0, 0);
  endOfWorkingDay.setHours(24, 0, 0, 0);

  return eventDate >= startOfWorkingDay && eventDate <= endOfWorkingDay;
};

const handleMessage = async ({ message }) => {
  try {
    const info = await registry.decode(message.value);

    const id = info.id;
    const mfc = parseFloat(info.data['mfc_id']);
    const driverId = info.data['driver_id'];
    const exitedAt = info.data['exited_at'];
    const modality = info.data['modality'];
    const queueSize = info.data['driver_partners_in_queue'];
    
    if (isTimestampInWorkingDay(exitedAt) && ukArr.includes(mfc)) {
      dataObj[id] = {};

      dataObj[id]['mfc_id'] = mfc;
      dataObj[id]['driver_id'] = driverId;
      dataObj[id]['timeStamp'] = exitedAt;
      dataObj[id]['modality'] = modality;
      dataObj[id]['queueSize'] = queueSize;

      // console.log('dataobj: ' + JSON.stringify(dataObj));


    }
  } catch (error) {
    console.error('Error handling message:', error);
  }
};

const processMessageQueue = async () => {
  if (!isProcessing) {
    isProcessing = true;
    while (messageQueue.length > 0) {
      const message = messageQueue.shift();
      await handleMessage({ message });
    }
    isProcessing = false;
  }
};

const checkLatest = (obj) => {
  const now = new Date();
  let latestTimestamp = null;

  for (const key in obj) {
    const timestamp = new Date(obj[key].timeStamp);
    if (!latestTimestamp || timestamp > latestTimestamp) {
      latestTimestamp = timestamp;
    }
  }

  const timeDifferenceInSeconds = (now - latestTimestamp) / 1000;

  return timeDifferenceInSeconds > 2;
};

const checkDataObjEmpty = (obj) => {
  return Object.keys(obj).length === 0;
};

const disconnectConsumer = async (lastMessageTime, isFirstMessage) => {
  try {
    if ((isFirstMessage && Date.now() - lastMessageTime >= 1 * 60 * 1000) || 
        checkLatest(dataObj) || 
        checkDataObjEmpty(dataObj)) {
      await consumer.disconnect();

      console.log(dataObj);

      // TRANSFORMED ARRAY -> for gsheet test
      
      const transformedArray = Object.keys(dataObj).map(key => ({
        "uuid": key,
        ...dataObj[key]
      }));

      console.log(transformedArray);

      // TRANSFORMED ARRAY
    }
  } catch (error) {
    console.error(`Failed to disconnect consumer: ${error}`);
  }

};

const runConsumer = async () => {
  try {
    await consumer.connect();
    await consumer.subscribe({ topic: 'ai.rideos.delivery.driver.exited_virtual_queue' });

    await consumer.run({
      eachMessage: async ({ message }) => {
        messageQueue.push(message);
        processMessageQueue();
      },
    });

    const intervalId = setInterval(() => {
      disconnectConsumer(Date.now(), messageQueue.length === 0);
      if (!messageQueue.length && !isProcessing) {
        clearInterval(intervalId);
      }
    }, 1 * 60 * 1000);
  } catch (err) {
    console.error(`Failed to run the consumer: ${err}`);
  }
};

runConsumer().catch(console.error);