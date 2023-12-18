'use strict';

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
    const topic = info.meta['event_name'];
    const ts = info.timestamp;
    const mfc = parseFloat(info.data['mfc_id']);
    
    if (isTimestampInWorkingDay(ts) && ukArr.includes(mfc)) {
      console.log(info.data)
      // switch(topic){
      //   case 'MfcOrderSupplyState':
      //     console.log(info.data)
      //     break;
        
      //   case 'ExitedVirtualQueue':
      //     console.log(info.data)
      //     break;
      // }
    }  

    /*const mfc = parseFloat(info.data['mfc_id']);
    const driverId = info.data['driver_id'];
    const exitedAt = info.data['exited_at'];
    const modality = info.data['modality'];
    const queueSize = info.data['driver_partners_in_queue'];
    
    if (isTimestampInWorkingDay(exitedAt) && ukArr.includes(mfc)) {
      // console.log(info)
      // console.log( mfc + ' ' + driverId + ' ' + exitedAt + ' '  + modality + ' ' + queueSize);
    }*/
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

// CONVERT Disconnect to a data regulator
// Writing our data to temp JSON Dump and then Gsheet using disconnect consumer -> to be substituted

const fs = require('fs');
const {google} = require("googleapis");
const auth = new google.auth.GoogleAuth({
      keyFile: "./service-account.json",
      scopes: "https://www.googleapis.com/auth/spreadsheets",
});


const runConsumer = async () => {
  try {
    await consumer.connect();
    await consumer.subscribe({ topics: ['ai.rideos.metrics.optimization.mfc_order_supply_state','ai.rideos.delivery.driver.exited_virtual_queue']});

    await consumer.run({
      eachMessage: async ({ message }) => {
        messageQueue.push(message);
        processMessageQueue();
      },
    });


  } catch (err) {
    console.error(`Failed to run the consumer: ${err}`);
  }
};

// final super function to run
runConsumer().catch(console.error);
