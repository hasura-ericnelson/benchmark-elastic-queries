/**
 * 
 * This script generates anonymous financial data and inserts it into two indices defined below as ACCOUNTS_INDEX_NAME and SUPERINDEX_INDEX_NAME
 * 
 *  Run me with:   
 * ES_USERNAME=<username> ES_PASSWORD=<password> ES_URL="https://localhost:9200" node generate-index-data.js 
 * 
 * 
*/

const { Client } = require('@elastic/elasticsearch');

// Replace with your Elasticsearch endpoint and index name
// const ELASTICSEARCH_NODE = 'https://ec2-34-235-138-126.compute-1.amazonaws.com:9200';
const ELASTICSEARCH_NODE = 'https://localhost:9200';

// small
// const ACCOUNTS_INDEX_NAME = '2sml_benchmark-accounts-domain';
// const SUPERINDEX_INDEX_NAME = '2sml_benchmark_supidx_position';

// large
const ACCOUNTS_INDEX_NAME = '1lg_benchmark-accounts-domain';
const SUPERINDEX_INDEX_NAME = '1lg_benchmark_supidx_position';

// Create Elasticsearch clients with SSL certificate verification disabled
const accountsClient = new Client({
  node: ELASTICSEARCH_NODE,
  auth: {
    username: process.env.ES_USERNAME,
    password: process.env.ES_PASSWORD,
  },
  tls: {
    rejectUnauthorized: false,
  },
});
const superindexClient = new Client({
  node: ELASTICSEARCH_NODE,
  auth: {
    username: process.env.ES_USERNAME,
    password: process.env.ES_PASSWORD,
  },
  tls: {
    rejectUnauthorized: false,
  },
});

// Constants
const TOTAL_DOCS = 1000000; //1 million
const NUM_ACCOUNTS = 10000;
const NUM_POSITIONS = 15000;
const NUM_DATES = 365;
const NUM_INSTRUMENTS = 100;
const MIN_POSITIONS_PER_ACCOUNT = 2000;
const MAX_POSITIONS_PER_ACCOUNT = 5000;

const BATCH_SIZE = 2000;
const MAX_CONCURRENT_BATCHES = 5;

// Helper functions
function randomDate(start, end) {
  const date = new Date(
    start.getTime() + Math.random() * (end.getTime() - start.getTime())
  );
  return date.toISOString().split('T')[0]; // 'YYYY-MM-DD'
}

function getRandomItem(array) {
  return array[Math.floor(Math.random() * array.length)];
}

// Function to send a batch to Elasticsearch
async function sendSuperindexBatchToElasticsearch(batch) {
  try {
    const response = await superindexClient.bulk({ body: batch });
    if (response.errors) {
      console.error('Superindex Bulk insert errors:', response.errors);
    } else {
      console.log(`Superindex Inserted batch of ${batch.length / 2} documents`);
    }
  } catch (err) {
    console.error('Superindex Error inserting batch:', err);
  }
}// Function to send a batch to Elasticsearch
async function sendAccountsBatchToElasticsearch(batch) {
  try {
    const response = await accountsClient.bulk({ body: batch });
    if (response.errors) {
      console.error('AcccountDomain Bulk insert errors:', response.errors);
    } else {
      console.log(`AccountDomain Inserted batch of ${batch.length / 2} documents`);
    }
  } catch (err) {
    console.error('AccountDomain Error inserting batch:', err);
  }
}

// Main function to generate and insert sample data
async function generateSampleData() {
  // Generate distinct values
  const accounts = Array.from({ length: NUM_ACCOUNTS }, (_, i) => `account_${i}`);
  const positions = Array.from({ length: NUM_POSITIONS }, (_, i) => `position_${i}`);
  const businessSystemCodes = Array.from({ length: 1000 }, (_, i) => `system_${i}`);
  const startDate = new Date(2020, 0, 1); // January 1, 2023
  const endDate = new Date(2023, 11, 31); // December 31, 2023
  const dates = Array.from({ length: NUM_DATES }, () => randomDate(startDate, endDate));
  const instruments = Array.from({ length: NUM_INSTRUMENTS }, (_, i) => `instrument_${i}`);
  const positionCcy = Array.from({ length: 100 }, (_, i) => `ccy_${i}`);
  const subledgerCodes = Array.from({ length: 100 }, (_, i) => `subledger_${i}`);
  const classificationLevels = Array.from({ length: 100 }, (_, i) => `class_${i}`);
  const currencies = Array.from({ length: 100 }, (_, i) => `currency_${i}`);
  const accountTypes = Array.from({ length: 100 }, (_, i) => `type_${i}`);
  const statuses = Array.from({ length: 100 }, (_, i) => `status_${i}`);

  let totalDocs = 0;
  let batches = [];
  let batch = [];

  for (let i = 0; i < NUM_ACCOUNTS; i++) {

    // IDs for accountId and positionId
    const accountId = accounts[i];
    const positionId = positions[i];


    const numPositions =
      Math.floor(
        Math.random() *
        (MAX_POSITIONS_PER_ACCOUNT - MIN_POSITIONS_PER_ACCOUNT + 1)
      ) + MIN_POSITIONS_PER_ACCOUNT;

    for (let j = 0; j < numPositions; j++) {
      const instrument = getRandomItem(instruments);
      const date = getRandomItem(dates);
      const businessSystemCode = getRandomItem(businessSystemCodes);
      const accountRecord = {
        Account: {
          accountId: accountId,
          businessSystemCode: businessSystemCode,
          accountType: getRandomItem(accountTypes),
          status: getRandomItem(statuses),
        }
      }

      const superindexRecord = {
        Account: accountRecord.Account,
        Position: {
          accountId: accountId,
          positionId: positionId,
          businessSystemCode: businessSystemCode,
          positionDate: date,
          instrumentId: instrument,
          positionCcy: getRandomItem(positionCcy),
          subledgerCode: getRandomItem(subledgerCodes),
          Instrument: {
            instrumentId: instrument,
            businessSystemCode: businessSystemCode,
          },
          InstrumentClassifications: {
            instrumentId: instrument,
            businessSystemCode: businessSystemCode,
            clasificationLevel1: getRandomItem(classificationLevels),
            clasificationLevel2: getRandomItem(classificationLevels),
            clasificationLevel3: getRandomItem(classificationLevels),
          },
          InstrumentPrice: {
            instrumentId: instrument,
            businessSystemCode: businessSystemCode,
            date: date,
            currency: getRandomItem(currencies),
            price: parseFloat((Math.random() * 990 + 10).toFixed(2)),
          },
          Taxlot: {
            accountId: accountId,
            instrumentCd: instrument,
            businessSystemCode: businessSystemCode,
            postedDate: date,
          },
          Transaction: {
            accountId: accountId,
            instrumentCd: instrument,
            businessSystemCode: businessSystemCode,
            transactionTimeStamp: new Date().toISOString(),
          },
        },
        Party: {
          accountId: accountId,
          businessSystemCode: businessSystemCode,
          status: getRandomItem(statuses)
        },
        Instrument: {
          instrumentId: instrument,
          businessSystemCode: businessSystemCode,
          clasificationLevel1: getRandomItem(classificationLevels),
          clasificationLevel2: getRandomItem(classificationLevels),
          clasificationLevel3: getRandomItem(classificationLevels),
        }
      };

      //*** Accounts START *************
      batch.push({ index: { _index: ACCOUNTS_INDEX_NAME } });
      batch.push(accountRecord);

      if (batch.length >= BATCH_SIZE * 2) {
        batches.push(batch);
        batch = [];

        if (batches.length >= MAX_CONCURRENT_BATCHES) {
          await Promise.all(batches.map(sendAccountsBatchToElasticsearch));
          batches = [];
        }
      }
      //*** Account Index END  **************
      //*** Superindex START   *************
      batch.push({ index: { _index: SUPERINDEX_INDEX_NAME } });
      batch.push(superindexRecord);

      if (batch.length >= BATCH_SIZE * 2) {
        batches.push(batch);
        batch = [];

        if (batches.length >= MAX_CONCURRENT_BATCHES) {
          await Promise.all(batches.map(sendSuperindexBatchToElasticsearch));
          batches = [];
        }
      }
      //******* SuperIndex END *********



      totalDocs += 1;
      if (totalDocs >= TOTAL_DOCS) {
        break;
      }
    }

    if (totalDocs >= TOTAL_DOCS) {
      break;
    }
  }

  // Insert remaining batches
  if (batch.length > 0) {
    batches.push(batch);
  }
  if (batches.length > 0) {
    await Promise.all(batches.map(sendAccountsBatchToElasticsearch));
    await Promise.all(batches.map(sendSuperindexBatchToElasticsearch));
  }

  console.log(`Total documents inserted: ${totalDocs}`);
}

// Run the function
generateSampleData().catch((error) => {
  console.error('Error generating sample data:', error);
});
