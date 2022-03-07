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

async function batchWrite(
  documentClient,
  tableName,
  items,
  retryTimeoutMinMs,
  retryTimeoutMaxMs
) {
  const chunkedItems = chunk(items, constants.MAX_ITEMS_PER_BATCH);

  const retryOptions = parseRetryOptions(retryTimeoutMinMs, retryTimeoutMaxMs);

  const runBatches = chunkedItems.map((batch, index) => {
    const batchWriteCommand = createBatchWriteCommand(tableName, batch);

    return execute(
      documentClient,
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
