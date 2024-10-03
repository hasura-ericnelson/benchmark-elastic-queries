const { Client } = require('@elastic/elasticsearch');


//small 
// const ACCOUNT_INDEX_NAME = '2sml_benchmark-accounts-domain';
//large
const ACCOUNT_INDEX_NAME = '1lg_benchmark-accounts-domain';

//small 
// const POSITIONS_SUPERINDEX_NAME = '2sml_benchmark_supidx_position';
// large
const POSITIONS_SUPERINDEX_NAME = '1lg_benchmark_supidx_position';

// Shoudl be something like 'system_514 or system_133'
// small data set
// const PREDICATE_BUSINESSYSTEMCODE = 'system_163';
// large dataset
const PREDICATE_BUSINESSYSTEMCODE = 'system_822';

const ELASTICSEARCH_NODE = 'https://localhost:9200';

const FETCH_SIZE = 10000;

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
  console.time(`query1|Fetch Account IDs matching predicate from PositionsSuperIndex TOTAL TIME:`); // Start timer
  console.log(`Query 1 - Fetch Account IDs matching Account.businessSystemCode==${predicate} from PositionsSuperIndex`);

  try {
    const accountIds = [];
    const sortField = '_doc'; // Use _doc for efficient sorting
    let searchAfter = null;
    let totalFetched = 0;
    let hasMore = true;

    // Open a Point In Time (PIT)
    const pitResponse = await client.openPointInTime({
      index: POSITIONS_SUPERINDEX_NAME,
      keep_alive: '2m',
    });
    const pitId = pitResponse.id;

    while (hasMore) {
      // Build the search parameters
      const searchParams = {
        body: {
          size: FETCH_SIZE,
          _source: ['Account.accountId'],
          query: {
            term: {
              'Account.businessSystemCode': predicate,
            },
          },
          sort: [{ [sortField]: 'asc' }],
          pit: {
            id: pitId,
            keep_alive: '2m',
          },
        },
      };

      // Include search_after only if it's not null
      if (searchAfter) {
        searchParams.body.search_after = searchAfter;
      }

      const response = await client.search(searchParams);

      const hits = response.hits.hits;
      if (hits.length === 0) {
        hasMore = false;
        break;
      }

      totalFetched += hits.length;
      accountIds.push(...hits.map(hit => hit._source.Account.accountId));

      // Update searchAfter with the sort values of the last hit
      searchAfter = hits[hits.length - 1].sort;

      console.log(`query1|Fetched ${totalFetched} account IDs so far...`);
    }

    // Close the PIT to free resources
    await client.closePointInTime({ body: { id: pitId } });

    console.log('query1|Total Account IDs fetched:', accountIds.length);
    return accountIds;
  } catch (error) {
    console.error('***query1|Error fetching account IDs:', error);
  } finally {
    console.timeEnd(`query1|Fetch Account IDs matching predicate from PositionsSuperIndex TOTAL TIME:`);
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
  const batchSize = 1000; // Adjust based on your system capacity
  const sortField = '_doc';
  let totalFetched = 0;

  // Open a Point In Time (PIT)
  const pitResponse = await client.openPointInTime({
    index: POSITIONS_SUPERINDEX_NAME,
    keep_alive: '2m',
  });
  const pitId = pitResponse.id;

  try {
    for (let i = 0; i < accountIds.length; i += batchSize) {
      const accountIdBatch = accountIds.slice(i, i + batchSize);
      let searchAfter = null;
      let hasMore = true;

      while (hasMore) {
        // Build the search parameters
        const searchParams = {
          body: {
            size: FETCH_SIZE,
            _source: ['Position.positionId', 'Position.accountId'],
            query: {
              terms: {
                'Position.accountId': accountIdBatch,
              },
            },
            sort: [{ [sortField]: 'asc' }],
            pit: {
              id: pitId,
              keep_alive: '2m',
            },
          },
        };

        // Include search_after only if it's not null
        if (searchAfter) {
          searchParams.body.search_after = searchAfter;
        }

        const response = await client.search(searchParams);

        const hits = response.hits.hits;
        if (hits.length === 0) {
          hasMore = false;
          break;
        }

        totalFetched += hits.length;
        positions.push(...hits.map(hit => hit._source.Position));

        // Update searchAfter with the sort values of the last hit
        searchAfter = hits[hits.length - 1].sort;

        console.log(`query2.2|Fetched ${totalFetched} positions so far...`);
      }
    }

    console.log('query2.2|Total Positions fetched:', positions.length);
    return positions;
  } catch (error) {
    console.error('query2.2|Error fetching positions:', error);
  } finally {
    // Close the PIT to free resources
    await client.closePointInTime({ body: { id: pitId } });
    console.timeEnd('query2.2|Fetch PositionIds from Positions Super Index');
    console.log(`---------`);
  }
}


async function fetchActIdsFromAcctDomainIndex(predicate) {
  console.log(`Query 2.1 - Fetching accountIds from Account Domain Index where Account.businessSystemCode == ${predicate}`);

  console.time('query2.1|Fetch Account IDs from Account Domain Index');

  const accountIds = [];
  const sortField = '_doc'; // Use _doc for efficient sorting
  let searchAfter = null;
  let totalFetched = 0;
  let hasMore = true;

  // Open a Point In Time (PIT)
  const pitResponse = await client.openPointInTime({
    index: ACCOUNT_INDEX_NAME,
    keep_alive: '2m',
  });
  const pitId = pitResponse.id;

  try {
    while (hasMore) {
      // Build the search parameters
      const searchParams = {
        body: {
          size: FETCH_SIZE,
          _source: ['Account.accountId'],
          query: {
            term: {
              'Account.businessSystemCode': predicate,
            },
          },
          sort: [{ [sortField]: 'asc' }],
          pit: {
            id: pitId,
            keep_alive: '2m',
          },
        },
      };

      // Include search_after only if it's not null
      if (searchAfter) {
        searchParams.body.search_after = searchAfter;
      }

      const response = await client.search(searchParams);

      const hits = response.hits.hits;
      if (hits.length === 0) {
        hasMore = false;
        break;
      }

      totalFetched += hits.length;
      accountIds.push(...hits.map(hit => hit._source.Account.accountId));

      // Update searchAfter with the sort values of the last hit
      searchAfter = hits[hits.length - 1].sort;

      console.log(`query2.1|Fetched ${totalFetched} account IDs so far...`);
    }

    console.log('query2.1|Total Account IDs fetched:', accountIds.length);
    return accountIds;
  } catch (error) {
    console.error('query2.1|Error fetching account IDs:', error);
  } finally {
    // Close the PIT to free resources
    await client.closePointInTime({ body: { id: pitId } });
    console.timeEnd('query2.1|Fetch Account IDs from Account Domain Index');
    console.log(`---------`);
  }
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

