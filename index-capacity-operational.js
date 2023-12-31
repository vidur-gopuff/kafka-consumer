'use strict';

//Include H3
const h3 = require('h3-js');

//Google config
const fs = require('fs');
const {google} = require("googleapis");

// const auth = new google.auth.GoogleAuth({
//       keyFile: "./service-account.json",
//       scopes: "https://www.googleapis.com/auth/spreadsheets",
// });

// const auth2 = new google.auth.GoogleAuth({
//   keyFile: "./gcp-service-account.json",
//   scopes: "https://www.googleapis.com/auth/spreadsheets",
// });

const auth3 = new google.auth.GoogleAuth({
  keyFile: "./gsheet1-service-account.json",
  scopes: "https://www.googleapis.com/auth/spreadsheets",
});

const client = auth3.getClient();
const googleSheets = google.sheets({ version: "v4", auth: client });
const spreadsheetId = "1A0pQBTsC052QbxcnH_qhDy0FtUQO2M2qHBURGUU02tM";

// Kafka config
const { Kafka } = require('kafkajs');
const { SchemaRegistry } = require('@kafkajs/confluent-schema-registry');
const { on } = require('events');
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

//Object Functions:
function recursivePush(dictionary, keyPath, value, index = 0) {
  if (index >= keyPath.length) return dictionary;

  const key = keyPath[index];

  if (index === keyPath.length - 1) {
    if (key in dictionary) {
      if (Array.isArray(dictionary[key])) {
        dictionary[key].push(value);
      } else {
        dictionary[key] = [value];
      }
    } else {
      dictionary[key] = [value];
    }
  } else {
    if (!(key in dictionary)) {
      dictionary[key] = {};
    }
    recursivePush(dictionary[key], keyPath, value, index + 1);
  }

  return dictionary;
}
function recursiveAdd(dictionary, keyPath, value, index = 0) {
  if (index >= keyPath.length) return dictionary;

  const key = keyPath[index];

  if (index === keyPath.length - 1) {
    dictionary[key] = value;
  } else {
    if (!(key in dictionary)) {
      dictionary[key] = {};
    }
    recursiveAdd(dictionary[key], keyPath, value, index + 1);
  }

  return dictionary;
}
function recursiveSum(dictionary, keyPath, value, index = 0) {
  if (index >= keyPath.length) return dictionary;

  const key = keyPath[index];

  if (index === keyPath.length - 1) {
    if (key in dictionary && typeof dictionary[key] === 'number') {
      dictionary[key] += value;
    } else {
      dictionary[key] = value;
    }
  } else {
    if (!(key in dictionary)) {
      dictionary[key] = {};
    }
    recursiveSum(dictionary[key], keyPath, value, index + 1);
  }

  return dictionary;
}


//Program Globals
let isProcessing = false;
const messageQueue = []; // Holds messages for processing
const ukArr = [1183, 1186, 1187, 1188, 1189, 1191, 1192, 1193, 1225, 1238, 1249, 1253, 1254,1291, 1292, 1294, 1301, 1302, 1308, 1309, 1317, 1318, 1319, 1321, 1323, 1352, 1353, 1368, 1369, 1375, 1380, 1381, 1383, 1391, 1398, 1537];
let dataObj={};


function isTimestampInWorkingDay(timestamp) {
  const eventDate = new Date(timestamp);
  const currentDate = new Date();

  const startOfWorkingDay = new Date(
    currentDate.getFullYear(),
    currentDate.getMonth(),
    currentDate.getDate() + (currentDate.getHours() < 3 ? -1 : 0),
    6, 0, 0, 0
  );

  const endOfWorkingDay = new Date(
    currentDate.getFullYear(),
    currentDate.getMonth(),
    currentDate.getDate() + (currentDate.getHours() < 3 ? 0 : 1),
    24, 0, 0, 0
  );

  return eventDate >= startOfWorkingDay && eventDate <= endOfWorkingDay;
}

async function handleMessage({ message }) {
  try {
    const info = await registry.decode(message.value);
    const ts = info.timestamp;
    
    if (isTimestampInWorkingDay(ts)) {
      const event = info.meta.event_name;

      switch (event) {
        case 'VelocityMetricsAggregated':
          handleVelocityMetricsAggregated(info.data,ts);
          break;
        case 'MfcOrderSupplyState':
          handleMFCState(info.data,ts);
          break;
        case 'DriverMarketplaceActivity': 
          handleMktplace(info.data,ts);
          break;
        case 'DriverStartedDelivery':
          handleDriverStartedDelivery(info.data,ts);
          break;
        case 'TripDurationEstimated':
          handleTripDuration(info.data,ts);
          break;
        case 'VehiclePositionUpdate':
          handlePosition(info.data,ts);
          break;
        case 'TripCompleted':
          handleTripComplete(info.data,ts);
          break;
        case 'EnteredVirtualQueue':
          handleDriverJoin(info.data,ts);
          break;
        default:
          console.log(event);
      }
    }
  } catch (error) {
    console.error('Error handling message:', error);
  }
}

async function processMessageQueue() {
  if (!isProcessing) {
    isProcessing = true;
    while (messageQueue.length > 0) {
      const message = messageQueue.shift();
      await handleMessage({ message });
    }
    isProcessing = false;
  }
}

async function runConsumer() {
  try {
    await consumer.connect();
    await consumer.subscribe({ topics: ['ai.rideos.delivery.driver.entered_virtual_queue','ai.rideos.delivery.trip.completed','ai.rideos.delivery.vehicle.position_updated','ai.rideos.delivery.trip.duration_estimated','ai.rideos.delivery.driver.started_delivery','ai.rideos.delivery.order.velocity_metrics_aggregated','ai.rideos.metrics.optimization.mfc_order_supply_state', 'ai.rideos.delivery.driver.marketplace_activity'] });

    await consumer.run({
      eachMessage: async ({ message }) => {
        messageQueue.push(message);
        processMessageQueue();
      },
    });

  } catch (err) {
    console.error(`Failed to run the consumer: ${err}`);
  }
}

function handleVelocityMetricsAggregated(jsonObj) {
  return;
}

function handleMFCState(jsonObj,ts) {
  const mfc = parseInt(jsonObj['mfc_id']);
  if (ukArr.includes(mfc)) {
    const newOrders = checkKey('new_orders',jsonObj,0);
    const ordersPacked = checkKey('orders_packed',jsonObj,0);
    const driversInQueue = checkKey('virtual_queue_size',jsonObj,0);
    const driversDelivering = checkKey('number_drivers_in_delivery',jsonObj,0);
    const driversReturning = checkKey('number_drivers_returning',jsonObj,0);
  
    dataObj = recursiveAdd(dataObj,['MFC Data',mfc,'Status'],{ts,newOrders,ordersPacked,driversInQueue,driversDelivering,driversReturning});
  }
}

function handleMktplace(jsonObj,ts) {
  let eventParts = jsonObj['event_id'].split('_');
  let mfc = parseInt(eventParts[0].split(':')[1]);
  if (ukArr.includes(mfc)) {
    let driverId = eventParts[1].split(':')[1];
    let tripId = eventParts[2].split(':')[1];
    let event_type = jsonObj['event_type']
    if (event_type == 'OFFER_ACCEPTED') {
      dataObj = recursiveAdd(dataObj,['Driver Data',mfc,driverId,'RejectionStreak'],0);
      dataObj = recursivePush(dataObj,['Driver Data',mfc,driverId,'RejectionStreaks'],1);
      dataObj = recursiveSum(dataObj,['Driver Data',mfc,driverId,'Accepted'],1);
      dataObj = recursiveAdd(dataObj,['Driver Data',mfc,driverId,'ActiveTrip','AcceptedAt'],ts);
      dataObj = recursiveAdd(dataObj,['Driver Data',mfc,driverId,'ActiveTrip','TripId'],tripId);
      dataObj = recursiveAdd(dataObj,['Trip Data',tripId,'MFC'],mfc);
      dataObj = recursiveAdd(dataObj,['Trip Data',tripId,'DriverId'],driverId);
    }
    if (event_type == 'OFFER_REJECTED') {
      dataObj = recursiveSum(dataObj,['Driver Data',mfc,driverId,'RejectionStreak'],1);
      dataObj = recursiveAdd(dataObj,['Driver Data',mfc,driverId,'ActiveTrip','TripId'],'---');
      dataObj = recursiveAdd(dataObj,['Driver Data',mfc,driverId,'ActiveTrip','AcceptedAt'],0);
    }
    //Record "our" drivers: - Handle in log on / log off???
    dataObj = recursiveAdd(dataObj,['EU Drivers',driverId],mfc);
  }

}

function handleDriverStartedDelivery(jsonObj,ts) {
  let tripId = jsonObj['trip_uuid'];
  if ('Trip Data' in dataObj) {
    if (tripId in dataObj['Trip Data']) {
      let driverId = dataObj['Trip Data'][tripId]['DriverId'];
      let mfc = dataObj['Trip Data'][tripId]['MFC'];
      dataObj = recursiveAdd(dataObj,['Driver Data',mfc,driverId,'ActiveTrip','StartedAt'],ts);
    }
  }
}

function handleTripDuration(jsonObj,ts) {
  let mfc = parseInt(jsonObj['mfc_id']);
  if (ukArr.includes(mfc)) {
    if ('Trip Data' in dataObj) {
      let tripId = jsonObj['trip_id'];
      if (tripId in dataObj['Trip Data']) {
        let totalDelivery = parseInt(jsonObj['total_trip_duration_in_seconds']);
        let returnTime = parseInt(jsonObj['return_to_mfc_travel_time_in_seconds']);
        let driverId = dataObj['Trip Data'][tripId]['DriverId'];
        let mfc = dataObj['Trip Data'][tripId]['MFC'];
        dataObj = recursiveAdd(dataObj,['Driver Data',mfc,driverId,'ActiveTrip','DriveTimeEstimate'],totalDelivery);
        dataObj = recursiveAdd(dataObj,['Driver Data',mfc,driverId,'ActiveTrip','ReturnTimeEstimate'],returnTime);
        dataObj = recursiveAdd(dataObj,['Driver Data',mfc,driverId,'ActiveTrip','UpdateTSForTimes'],ts);
      }
    }
  }
}

function handlePosition(jsonObj,ts) {
  let driverId = jsonObj['driver_id'];
  if (['EU Drivers'] in dataObj) {
    if (driverId in dataObj['EU Drivers']) {
      let mfc = dataObj['EU Drivers'][driverId];
      let lat = jsonObj['latitude'];
      let lng = jsonObj['longitude'];
      const hex = h3.latLngToCell(lat,lng,9);
      dataObj = recursiveAdd(dataObj,['Driver Data',mfc,driverId,'CurrentHex'],hex);
    }
  }
}

function handleTripComplete(jsonObj,ts) {
  let tripId = jsonObj['trip_id'];
  if ('Trip Data' in dataObj) {
    if (tripId in dataObj['Trip Data']) {
      let driverId = dataObj['Trip Data'][tripId]['DriverId'];
      let mfc = dataObj['Trip Data'][tripId]['MFC'];      
      if (ukArr.includes(mfc)) {      
        dataObj = recursiveAdd(dataObj,['Driver Data',mfc,driverId,'ActiveTrip','EndedAt'],ts);
      }
    }
  }
}

function handleDriverJoin(jsonObj,ts) {
  let mfc = parseInt(jsonObj['mfc_id']);
  if (ukArr.includes(mfc)) {
    let driverId = (jsonObj['driver_id']);
    let modality = (jsonObj['modality']);
    dataObj = recursiveAdd(dataObj,['Driver Data',mfc,driverId,'Modality'],modality);
  }  
  let a = 1;
}

function checkKey(key,obj,returnIfNotPresent) {
  if (key in obj) {
    return obj[key];
  } else {
    return returnIfNotPresent;
  }
}


function heartbeat() {
  try {
    let localData = JSON.parse(JSON.stringify(dataObj));
    
    // console.log(localData['MFC Data']);
    
    if (localData['MFC Data'] != undefined) {
      let persistentArray;
      try {
        const persistentData = fs.readFileSync('persistent-mfc-data.json', 'utf8');
        persistentArray = JSON.parse(persistentData);
      }  
      catch {
        persistentArray = {};
      }
      
      console.log(persistentArray)
      
      // key is the mfc

      const localArray = Object.entries(localData['MFC Data']).map(([key, value]) => {
        const {
          ts,
          newOrders,
          ordersPacked,
          driversInQueue,
          driversDelivering,
          driversReturning,
        } = value.Status;
        return [key, ts, newOrders, ordersPacked, driversInQueue, driversDelivering, driversReturning];
      });

      console.log(localArray);

      // // regular loop

      // localArray.forEach(localItem => {
      //   const idB = localItem[0];
      //   const timestampB = new Date(localItem[1]);
      
      //   persistentArray.forEach((persistentItem, index) => {
      //     const timestampA = new Date(persistentItem[1]);
      //     const idA = persistentItem[0];
      
      //     if (idA === idB && timestampA < timestampB) {
      //       persistentArray[index] = localItem;
      //     }
      //   });
      // });

      // faster loop

      for (const localItem of localArray) {
        const idB = localItem[0];
        const timestampB = new Date(localItem[1]);

        for (let index = 0; index < persistentArray.length; index++) {
          const persistentItem = persistentArray[index];
          const timestampA = new Date(persistentItem[1]);
          const idA = persistentItem[0];

          if (idA === idB && timestampA < timestampB) {
            persistentArray[index] = localItem;
            break; // Exit the loop once the condition is met
          }
        }
      }

      // sort persistent array
      
      const sortedPersistentArray = persistentArray.sort((a, b) => {
        const idA = a[0];
        const idB = b[0];
      
        if (idA < idB) {
          return -1;
        } else if (idA > idB) {
          return 1;
        }
        return 0;
      });
      
      fs.writeFileSync('persistent-mfc-data.json', JSON.stringify(sortedPersistentArray, null, 2), 'utf8');

      googleSheets.spreadsheets.values.update(
        {
          auth: auth3,
          spreadsheetId: spreadsheetId,
          range: "mfcCapacity!A2",
          valueInputOption: "USER_ENTERED",
          resource: {majorDimension: "ROWS", values: sortedPersistentArray},
        }
      );
    }
    setTimeout(heartbeat, heartbeatEvery*1000);
  } catch {
    setTimeout(heartbeat, heartbeatEvery*1000);
  }
}

// final super function to run
const heartbeatEvery = 1 // seconds value
setTimeout(heartbeat, heartbeatEvery*1000);
runConsumer().catch(console.error);
