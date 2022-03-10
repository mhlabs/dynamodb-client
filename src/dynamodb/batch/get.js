const { BatchGetCommand } = require('@aws-sdk/lib-dynamodb');

const { chunk } = require('../../array/chunk');
const constants = require('./constants');

const execute = require('./execute');
const parseRetryOptions = require('./retry-options');

function createBatchGetCommand(tableName, batch, options) {
  const command = new BatchGetCommand({
    RequestItems: {
      [tableName]: {
        Keys: batch,
        ...options
      }
    }
  });

  return command;
}

function ensureValidParameters(documentClient, tableName, keys) {
  if (!documentClient) throw new Error('documentClient is required.');
  if (!tableName) throw new Error('tableName is required.');
  if (!keys) throw new Error('Key list is required.');
  if (!keys.every((key) => typeof key === 'object'))
    throw new Error('All keys should be objects.');
}

function hasUnprocessedKeys(response) {
  return (
    response?.UnprocessedKeys &&
    Object.keys(response?.UnprocessedKeys).length > 0
  );
}

async function batchGet(
  documentClient,
  tableName,
  keys,
  options,
  retryTimeoutMinMs,
  retryTimeoutMaxMs
) {
  ensureValidParameters(documentClient, tableName, keys);

  if (!keys.length) return [];

  const chunkedItems = chunk(keys, constants.MAX_KEYS_PER_BATCH_GET);

  const retryOptions = parseRetryOptions(retryTimeoutMinMs, retryTimeoutMaxMs);

  const runBatches = chunkedItems.map((batch, index) => {
    const batchGetCommand = createBatchGetCommand(tableName, batch, options);
    return execute(documentClient, batchGetCommand, index + 1, 0, retryOptions);
  });

  const responses = await Promise.all(runBatches);

  const items = [];

  return items;
}

module.exports = batchGet;
