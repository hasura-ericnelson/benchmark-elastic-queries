const { Client } = require('@elastic/elasticsearch');


const ACCOUNT_INDEX_NAME = '2sml_benchmark-accounts-domain';
const POSITIONS_SUPERINDEX_NAME = '2sml_benchmark_supidx_position';

// Shoudl be something like 'system_514 or system_133'
const PREDICATE_BUSINESSYSTEMCODE = 'system_163';

const ELASTICSEARCH_NODE = 'https://localhost:9200';
const FETCH_BATCH_SIZE = 10000;
const ES_SCROLL_TIMEOUT = '2m'
const POSITION_BATCH_SIZE = FETCH_BATCH_SIZE;

// Create Elasticsearch client with SSL certificate verification disabled
const client = new Client({
  node: ELASTICSEARCH_NODE,
  auth: {
    username: process.env.ES_USERNAME,
    password: process.env.ES_PASSWORD,
  },
  tls: {
    rejectUnauthorized: false,
  },
});

//const getRandom = (max) => Math.floor(Math.random() * max) + 1;


// select accountIds only from Superindex
async function query1FetchActIdsFromSuperIndex(predicate) {
  console.time(`query1|Fetch Account IDs matching predicate from PositionsSuperIndex TOTAL TIME:`); // Start timer for the first query
  console.log(`Query 1 - Fetch Account IDs matching Account.businessSystemCode==${predicate} from PositionsSuperIndex`);
  try {
    const accountIds = [];
    let totalFetched = 0;
    let batchCount = 0;

    // Initial search request to start scrolling
    let response = await client.search({
      index: POSITIONS_SUPERINDEX_NAME,
      scroll: ES_SCROLL_TIMEOUT,
      body: {
        _source: ['Account.accountId'], // Specify the fields to include
        query: {
          term: {
            'Account.businessSystemCode': predicate,
          },
        },
      },
      size: FETCH_BATCH_SIZE, // Fetch documents per batch
    });
    batchCount++;
    const hits = response.hits.hits;
    const totalHits = response.hits.total.value;
    totalFetched += hits.length;
    accountIds.push(...hits.map(hit => hit._source.Account.accountId));

    console.log(`query1|Batch ${batchCount}: Fetched ${hits.length} account IDs. Total fetched: ${totalFetched}/${totalHits}`);

    // Continue scrolling until no more hits are returned
    while (hits.length > 0) {
      response = await client.scroll({
        scroll_id: response._scroll_id,
        scroll: ES_SCROLL_TIMEOUT,
      });

      if (response.hits.hits.length === 0) {
        break;
      }

      batchCount++;
      const hits = response.hits.hits;
      totalFetched += hits.length;
      accountIds.push(...hits.map(hit => hit._source.Account.accountId));

      console.log(`query1|Batch ${batchCount}: Fetched ${hits.length} account IDs. Total fetched: ${totalFetched}/${totalHits}`);
    }

    // Clear the scroll context to free up resources
    await client.clearScroll({
      scroll_id: response._scroll_id,
    });


    // console.log(`SanityCheck-> Random response 1`)
    // randomIndex1 = getRandom(accountIds.length)
    // console.log(`\accountIds[${randomIndex1}] = ${JSON.stringify(accountIds[randomIndex1])}`)
    // console.log(`SanityCheck-> Random response 2`)
    // randomIndex2 = getRandom(accountIds.length)
    // console.log(`\accountIds[${randomIndex2}] = ${JSON.stringify(accountIds[randomIndex2])}`)


    console.log('query1|Total Account IDs fetched:', accountIds.length);
    return accountIds;
  } catch (error) {
    console.error('***query1|Error fetching account IDs:', error);
  } finally {
    console.timeEnd(`query1|Fetch Account IDs matching predicate from PositionsSuperIndex TOTAL TIME:`); // End timer for the first query
    console.log(`---------`);
  }
}


// fetch list of accountids where account.businessSystemCode == system_4 from AccountDomainIndex
//  THEN
// fetch positions where position.accountId IN [accountIds]
async function query2(predicate) {
  console.time('query2|Total Execution Time'); // Start overall timer
  try {
    const accountIdsFromAcctDomainIndex = await fetchActIdsFromAcctDomainIndex(predicate);
    console.log('query2|Total Account IDs fetched:', accountIdsFromAcctDomainIndex.length);


    if (accountIdsFromAcctDomainIndex.length === 0) {
      console.log('query2|No account IDs found. Exiting.');
      return [];
    }

    console.time('query2.2');
    const positions = await fetchPositionsForAcctIdsListFromAcctDomainIndex(accountIdsFromAcctDomainIndex);

    console.log('query2|Total Positions fetched:', positions.length);
    console.timeEnd('query2.2');

    console.timeEnd('query2|Total Execution Time');
    console.log(`---------`);
    return positions; // Return the fetched positions as needed
  } catch (error) {
    console.error('query2|Error in query2 function:', error);
  }
}


async function fetchPositionsForAcctIdsListFromAcctDomainIndex(accountIds) {
  console.log(`Query 2.2 - Fetching Position.positionId & Position.accountId from Positions Super Index where Position.accountId IN [${accountIds.length} accountIds passed in]`);

  console.time('query2.2|Fetch PositionIds from Positions Super Index');
  const positions = [];
  let totalFetchedPositions = 0;
  let positionBatchCount = 0;
  // Number of accountIds per batch

  // **Step 2: Fetch positions from PositionsDomainIndex**
  // Split accountIds into batches to avoid exceeding terms query limits
  //TODO: Do we need batching here?
  for (let i = 0; i < accountIds.length; i += POSITION_BATCH_SIZE) {
    const accountIdBatch = accountIds.slice(i, i + POSITION_BATCH_SIZE);

    // Initial search request for positions
    let posResponse = await client.search({
      index: POSITIONS_SUPERINDEX_NAME, // Replace with your positions index name
      scroll: ES_SCROLL_TIMEOUT,
      body: {
        _source: ['Position.positionId', 'Position.accountId'], // Adjust fields as needed
        query: {
          terms: {
            'Position.accountId': accountIdBatch,
          },
        },
      },
      size: FETCH_BATCH_SIZE,
    });

    positionBatchCount++;
    let posHits = posResponse.hits.hits;
    const totalPositionHits = posResponse.hits.total.value;
    totalFetchedPositions += posHits.length;
    positions.push(...posHits.map(hit => hit._source.Position));

    console.log(
      `query2|Position Batch ${positionBatchCount}: Fetched ${posHits.length} positions. Total fetched: ${totalFetchedPositions}/${totalPositionHits}`
    );

    // Continue scrolling for positions
    while (posHits.length > 0) {
      posResponse = await client.scroll({
        scroll_id: posResponse._scroll_id,
        scroll: ES_SCROLL_TIMEOUT,
      });

      posHits = posResponse.hits.hits;
      if (posHits.length === 0) {
        break;
      }

      totalFetchedPositions += posHits.length;
      positions.push(...posHits.map(hit => hit._source.Position));

      console.log(
        `query2|Position Batch ${positionBatchCount}: Fetched additional ${posHits.length} positions. Total fetched: ${totalFetchedPositions}/${totalPositionHits}`
      );
    }

    // Clear the scroll context for positions
    await client.clearScroll({
      scroll_id: posResponse._scroll_id,
    });
  }
  console.timeEnd('query2.2|Fetch PositionIds from Positions Super Index');
  console.log(`---------`);
  return positions;
}

async function fetchActIdsFromAcctDomainIndex(predicate) {
  // const ES_SCROLL_TIMEOUT = '2m'; // Scroll context valid for 2 minutes
  // const FETCH_BATCH_SIZE = 1000; // Adjust based on your system's capacity
  console.log(`Query 2.1 - Fetching accountIds from Account Domain Index where Account.businessSystemCode == ${predicate}`);

  // **Step 1: Fetch accountIds from AccountDomainIndex**
  console.time('query2.1|Fetch Account IDs from Account Domain Index');
  const accountIds = [];
  let totalFetchedAccounts = 0;
  let accountBatchCount = 0;

  let response = await client.search({
    index: ACCOUNT_INDEX_NAME,
    scroll: ES_SCROLL_TIMEOUT,
    body: {
      _source: ['Account.accountId'], // Only fetch the accountId field
      query: {
        term: {
          'Account.businessSystemCode': predicate,
        },
      },
    },
    size: FETCH_BATCH_SIZE,
  });

  accountBatchCount++;
  let hits = response.hits.hits;
  const totalAccountHits = response.hits.total.value;
  totalFetchedAccounts += hits.length;
  accountIds.push(...hits.map(hit => hit._source.Account.accountId));

  console.log(
    `query2|Account Batch ${accountBatchCount}: Fetched ${hits.length} account IDs. Total fetched: ${totalFetchedAccounts}/${totalAccountHits}`
  );

  // Continue scrolling for accounts
  while (hits.length > 0) {
    response = await client.scroll({
      scroll_id: response._scroll_id,
      scroll: ES_SCROLL_TIMEOUT,
    });

    hits = response.hits.hits;
    if (hits.length === 0) {
      break;
    }

    accountBatchCount++;
    totalFetchedAccounts += hits.length;
    accountIds.push(...hits.map(hit => hit._source.Account.accountId));

    console.log(
      `query2|Account Batch ${accountBatchCount}: Fetched ${hits.length} account IDs. Total fetched: ${totalFetchedAccounts}/${totalAccountHits}`
    );
  }

  // Clear the scroll context for accounts
  await client.clearScroll({
    scroll_id: response._scroll_id,
  });
  console.timeEnd('query2.1|Fetch Account IDs from Account Domain Index');
  console.log(`---------`);
  return accountIds;
}

async function main() {
  console.time('main|Total Execution Time'); // Start overall timer

  try {
    // Measure time for fetching account IDs
    const accountStart = Date.now();
    const accountIdsFromPositionsSuperIndex = await query1FetchActIdsFromSuperIndex(PREDICATE_BUSINESSYSTEMCODE);
    const accountEnd = Date.now();

    // Measure time for fetching position IDs
    const positionStart = Date.now();
    const positionIds = await query2(PREDICATE_BUSINESSYSTEMCODE);
    // const positionIds = await fetchPositionIds(accountIds);
    const positionEnd = Date.now();

    // Calculate time differences
    const accountQueryTime = accountEnd - accountStart;
    const positionQueryTime = positionEnd - positionStart;
    const timeDifference = positionQueryTime - accountQueryTime;

    console.log(`main|accountIds.length: ${accountIdsFromPositionsSuperIndex.length}`);
    console.log(`main|positionIds.length: ${positionIds.length}`);

    console.log(`main|Time taken for fetching account IDs: ${accountQueryTime} ms`);
    console.log(`main|Time taken for fetching position IDs: ${positionQueryTime} ms`);
    console.log(`main|Difference in execution time: ${timeDifference} ms`);
  } catch (error) {
    console.error('main|Error in the main function:', error);
  } finally {
    console.timeEnd('main|Total Execution Time'); // End overall timer
  }
}

// Call the main function
main();

