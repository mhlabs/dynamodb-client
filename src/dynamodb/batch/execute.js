const { BatchGetCommand, BatchWriteCommand } = require('@aws-sdk/lib-dynamodb');

const { randomInteger } = require('../../randomizer/random-integer');
const constants = require('./constants');

function needsRetry(response) {
  const hasUnprocessededKeys =
    response?.UnprocessedKeys &&
    Object.keys(response?.UnprocessedKeys).length > 0;

  const hasUnprocessededItems =
    response?.UnprocessedItems &&
    Object.keys(response?.UnprocessedItems).length > 0;

  return hasUnprocessededKeys || hasUnprocessededItems;
}

async function retryUnprocessedItems(
  documentClient,
  tableName,
  response,
  batchNo,
  retryAttemptNo,
  retryOptions,
  previousItems
) {
  if (retryAttemptNo > constants.UNPROCESSED_ITEMS_RETRY_LIMIT) {
    const unprocessed = response.UnprocessedItems
      ? 'UnprocessedItems'
      : 'UnprocessedKeys';

    throw new Error(
      `Batch: ${batchNo} - returned ${unprocessed} after ${retryAttemptNo} attempts (${
        retryAttemptNo - 1
      } retries)`
    );
  }

  const randomTimeoutToAvoidThrottling = randomInteger(
    retryOptions.minMs,
    retryOptions.maxMs
  );

  console.log(
    `UnprocessedItems, retrying after waiting ${randomTimeoutToAvoidThrottling} ms, attempt ${retryAttemptNo})...`
  );

  await new Promise((r) => {
    setTimeout(r, randomTimeoutToAvoidThrottling);
  });

  let command;

  if (response.UnprocessedItems) {
    command = new BatchWriteCommand({
      RequestItems: response.UnprocessedItems
    });
  } else if (response.UnprocessedKeys) {
    command = new BatchGetCommand({
      RequestItems: response.UnprocessedKeys
    });
  } else {
    throw new Error('Retry called without any unprocessed items or keys.');
  }

  const items = await execute(
    documentClient,
    tableName,
    command,
    batchNo,
    retryAttemptNo,
    retryOptions,
    previousItems
  );

  return items;
}

async function execute(
  documentClient,
  tableName,
  batchCommand,
  batchNo,
  retryCount,
  retryOptions,
  previousItems = []
) {
  let items = [...previousItems];
  const response = await documentClient.send(batchCommand);

  if (response.Responses && response.Responses[tableName]) {
    items.push(...response.Responses[tableName]);
  }

  if (!needsRetry(response)) return items;

  items = await retryUnprocessedItems(
    documentClient,
    tableName,
    response,
    batchNo,
    retryCount + 1,
    retryOptions,
    items
  );

  return items;
}

module.exports = execute;
