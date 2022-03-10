const { BatchGetCommand } = require('@aws-sdk/lib-dynamodb');

const { chunk } = require('../../array/chunk');
const constants = require('./constants');

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
  options = undefined,
  retryAttempt = 0
) {
  if (retryAttempt > 1) {
    throw new Error('batchGet error: returned UnprocessedKeys after retry');
  }

  ensureValidParameters(documentClient, tableName, keys);

  const chunkedItems = chunk(keys, constants.MAX_KEYS_PER_BATCH_GET);

  const runBatches = chunkedItems.map((keyBatch) => {
    const command = new BatchGetCommand({
      RequestItems: {
        [tableName]: {
          Keys: keyBatch,
          ...options
        }
      }
    });

    return documentClient.send(command);
  });

  const responses = await Promise.all(runBatches);

  const items = [];
  const unprocessedKeys = [];

  responses.forEach((response) => {
    items.push(...response.Responses[tableName]);
    if (hasUnprocessedKeys(response)) {
      unprocessedKeys.push(...response.UnprocessedKeys[tableName].Keys);
    }
  });

  if (!unprocessedKeys.length) return items;

  const retryItems = await batchGet(
    documentClient,
    tableName,
    unprocessedKeys,
    options,
    retryAttempt + 1
  );

  items.push(...retryItems);

  return items;
}

module.exports = batchGet;
