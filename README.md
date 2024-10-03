# Elasticsearch Benchmarking Script

This script benchmarks two different methods of fetching data from Elasticsearch:

1. **Denormalized Fetch**: Fetching account IDs directly from the positions superindex based on a predicate.
2. **Normalized Fetch**: Fetching account IDs from the accounts domain index based on a predicate, then fetching positions from the positions superindex using those account IDs.

The goal is to compare the performance and efficiency of these two approaches, especially when dealing with large datasets that exceed Elasticsearch's default `size` limit.

---

## Table of Contents

- [Prerequisites](#prerequisites)
- [Overview](#overview)
- [Script Functions](#script-functions)
- [Usage](#usage)
- [Configuration](#configuration)
- [Benchmarking Methodology](#benchmarking-methodology)
- [Handling Large Result Sets](#handling-large-result-sets)
- [Notes](#notes)
- [License](#license)

---

## Prerequisites

- **Node.js** installed on your system.
- Access to an Elasticsearch cluster (version 7.10 or higher recommended).
- Necessary permissions to query the indices.
- Elasticsearch indices with the expected data structure.

---

## Overview

The script performs the following steps:

1. **Query 1**: Fetches account IDs directly from the positions superindex where `Account.businessSystemCode` matches a given predicate.
2. **Query 2**:
   - Fetches account IDs from the accounts domain index where `Account.businessSystemCode` matches the same predicate.
   - Uses the fetched account IDs to query the positions superindex for positions matching those account IDs.

By timing these operations, the script provides insights into the performance differences between querying a denormalized index versus a normalized approach.

---

## Script Functions

### `query1FetchActIdsFromSuperIndex(predicate)`

- **Purpose**: Fetches account IDs from the positions superindex where `Account.businessSystemCode` equals the given predicate.
- **Method**:
  - Uses Elasticsearch's `search_after` parameter and Point-In-Time (PIT) API for efficient deep pagination.
  - Iteratively fetches all matching account IDs, overcoming the default size limit.
- **Logging**: Measures and logs the time taken and progress of the query.

### `query2(predicate)`

- **Purpose**: Performs a two-step normalized fetch.
  1. **Fetch Account IDs**: Calls `fetchActIdsFromAcctDomainIndex(predicate)` to fetch account IDs from the accounts domain index.
  2. **Fetch Positions**: Calls `fetchPositionsForAcctIdsListFromAcctDomainIndex(accountIds)` to fetch positions from the positions superindex using the account IDs.
- **Logging**: Measures and logs the time taken for each step and the total execution time.

### `fetchActIdsFromAcctDomainIndex(predicate)`

- **Purpose**: Fetches account IDs from the accounts domain index where `Account.businessSystemCode` equals the given predicate.
- **Method**:
  - Uses Elasticsearch's `search_after` parameter and PIT API.
  - Iteratively fetches all matching account IDs.
- **Logging**: Tracks progress and total fetched account IDs.

### `fetchPositionsForAcctIdsListFromAcctDomainIndex(accountIds)`

- **Purpose**: Fetches positions from the positions superindex where `Position.accountId` is in the provided list of account IDs.
- **Method**:
  - Batches the account IDs to avoid exceeding the `terms` query limit.
  - Uses `search_after` and PIT API to fetch all matching positions.
- **Logging**: Tracks progress and total fetched positions.

### `main()`

- **Purpose**: Orchestrates the execution of the benchmarking process.
- **Method**:
  - Calls `query1FetchActIdsFromSuperIndex(predicate)` and `query2(predicate)`.
  - Measures and logs the execution times and results.

---

## Usage

### Generating Sample Data

To generate and insert sample data into Elasticsearch:

```
ES_USERNAME=<username> ES_PASSWORD=<password> ES_URL="https://localhost:9200" node generate-index-data.js
```

Replace `<username>`, `<password>`, and the URL with your Elasticsearch credentials and endpoint.

### Running Benchmarks

There are two benchmark scripts available:

1. Batching strategy:

   ```
   node 2-query-benchmarks-batching.js
   ```

2. Pagination strategy:
   ```
   node 2-query-benchmarks-paginate.js
   ```

## Project Structure

- `generate-index-data.js`: Script to generate and insert sample data into Elasticsearch.
- `2-query-benchmarks-batching.js`: Benchmark script using a batching strategy.
- `2-query-benchmarks-paginate.js`: Benchmark script using a pagination strategy.

## Configuration

You can modify the following constants in the scripts to adjust the behavior:

- `TOTAL_DOCS`: Total number of documents to generate
- `NUM_ACCOUNTS`: Number of unique accounts
- `NUM_POSITIONS`: Number of unique positions
- `BATCH_SIZE`: Number of documents per batch when inserting
- `MAX_CONCURRENT_BATCHES`: Maximum number of concurrent batch insertions
  Modify the constants at the beginning of the script to match your environment:

```javascript
// Indices
const ACCOUNT_INDEX_NAME = "1lg_benchmark-accounts-domain";
const POSITIONS_SUPERINDEX_NAME = "1lg_benchmark_supidx_position";

// Predicate
const PREDICATE_BUSINESSYSTEMCODE = "system_528";

// Elasticsearch Node
const ELASTICSEARCH_NODE = "https://localhost:9200";

// Fetch Size
const FETCH_SIZE = 1000; // Adjust based on performance tests
```

### Review the Output

The script will output the execution times and the number of records fetched for each query.

---

## Configuration

- **Indices**: Ensure that the indices specified in the constants exist in your Elasticsearch cluster and contain the expected data.

- **Fetch Size**: The `FETCH_SIZE` constant determines the number of records fetched in each request. Adjust this value based on your cluster's capacity and Elasticsearch's limitations.

---

## Benchmarking Methodology

The script compares two methods:

### **Denormalized Fetch (Query 1)**

- Fetches account IDs directly from the positions superindex based on a business system code.
- Uses the PIT API and `search_after` for efficient deep pagination.
- This approach treats the data as denormalized, with all necessary information available in a single index.

### **Normalized Fetch (Query 2)**

- First fetches account IDs from the accounts domain index based on the business system code.
- Then fetches positions from the positions superindex using the list of account IDs.
- Also uses the PIT API and `search_after` for deep pagination.
- This approach simulates a normalized data model, where related data is stored in separate indices.

By measuring the execution times and comparing the results, you can assess the performance implications of each approach.

---

## Handling Large Result Sets

### Elasticsearch Limitations

- Elasticsearch limits the maximum number of results that can be retrieved in a single search request (default is 10,000).
- To fetch all matching documents, the script uses:
  - **Point-In-Time (PIT) API**: Provides a consistent view of data at a specific point in time.
  - **`search_after` Parameter**: Allows efficient deep pagination without the limitations of the `from` parameter.

### Implementation Details

- **PIT Initialization**: Before starting the search, the script opens a PIT context that remains valid throughout the pagination process.
- **Conditional `search_after`**: The `search_after` parameter is included only after the first request, using the sort values from the last hit.
- **Sorting**: Uses `_doc` for sorting to improve performance. For consistency, especially if the index is updated during the search, consider sorting by a unique field like `_id`.
- **Batching**: For queries involving large lists (e.g., account IDs), the script batches requests to avoid exceeding query limits.

---

## Notes

- **Error Handling**: The script includes error handling to capture and log any issues that occur during execution.

- **Version Compatibility**: The PIT and `search_after` functionalities require Elasticsearch version **7.10** or higher.

- **Performance Considerations**:

  - Fetching large datasets can be resource-intensive. Monitor your cluster's performance and adjust the `FETCH_SIZE` and other parameters as needed.
  - The script includes logging statements to help track progress and identify potential bottlenecks.

- **Testing**: Before running the script on large datasets, test it with smaller datasets to ensure it behaves as expected.

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

**Disclaimer**: Use this script responsibly. Ensure you have appropriate permissions and that running this script won't adversely affect your Elasticsearch cluster's performance or stability.
