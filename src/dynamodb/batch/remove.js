const { BatchWriteCommand } = require('@aws-sdk/lib-dynamodb');

const { chunk } = require('../../array/chunk');
const constants = require('./constants');
const execute = require('./execute');
const parseRetryOptions = require('./retry-options');

function createBatchDeleteCommand(tableName, batch) {
  const putRequests = batch.map((key) => ({
    DeleteRequest: {
      Key: key
    }
  }));
  return new BatchWriteCommand({
    RequestItems: {
      [tableName]: putRequests
    }
  });
}

function ensureValidParameters(documentClient, tableName, keys) {
  if (!documentClient) throw new Error('documentClient is required.');
  if (!tableName) throw new Error('Table name is required.');
  if (!keys) throw new Error('Key list is required.');

  if (!keys.every((key) => typeof key === 'object'))
    throw new Error('Keys must be objects.');
}

async function batchRemove(
  documentClient,
  tableName,
  keys,
  retryTimeoutMinMs,
  retryTimeoutMaxMs
) {
  ensureValidParameters(documentClient, tableName, keys);

  if (!keys.length) return true;

  const chunkedItems = chunk(keys, constants.MAX_ITEMS_PER_BATCH);

  const retryOptions = parseRetryOptions(retryTimeoutMinMs, retryTimeoutMaxMs);

  const runBatches = chunkedItems.map((batch, index) => {
    const batchWriteCommand = createBatchDeleteCommand(tableName, batch);

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

module.exports = batchRemove;
