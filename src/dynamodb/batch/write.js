const { BatchWriteCommand } = require('@aws-sdk/lib-dynamodb');

const { chunk } = require('../../array/chunk');
const constants = require('./constants');
const execute = require('./execute');
const parseRetryOptions = require('./retry-options');

function createBatchWriteCommand(tableName, batch) {
  const putRequests = batch.map((item) => ({
    PutRequest: {
      Item: item
    }
  }));
  return new BatchWriteCommand({
    RequestItems: {
      [tableName]: putRequests
    }
  });
}

function ensureValidParameters(documentClient, tableName, items) {
  if (!documentClient) throw new Error('documentClient is required.');
  if (!tableName) throw new Error('Table name is required.');
  if (!items) throw new Error('Item list is required.');
}

async function batchWrite(
  documentClient,
  tableName,
  items,
  retryTimeoutMinMs,
  retryTimeoutMaxMs
) {
  ensureValidParameters(documentClient, tableName, items);

  if (!items.length) return true;

  const chunkedItems = chunk(items, constants.MAX_ITEMS_PER_BATCH_WRITE);

  const retryOptions = parseRetryOptions(retryTimeoutMinMs, retryTimeoutMaxMs);

  const runBatches = chunkedItems.map((batch, index) => {
    const batchWriteCommand = createBatchWriteCommand(tableName, batch);

    return execute(
      documentClient,
      tableName,
      batchWriteCommand,
      index + 1,
      0,
      retryOptions
    );
  });

  await Promise.all(runBatches);

  return true;
}

module.exports = batchWrite;
