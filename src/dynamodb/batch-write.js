const { BatchWriteCommand } = require('@aws-sdk/lib-dynamodb');

const { chunk } = require('../array/chunk');
const { randomInteger } = require('../randomizer/random-integer');

const MAX_ITEMS_PER_BATCH = 25;
const UNPROCESSED_RETRY_LIMIT = 2;

const DEFAULT_UNPROCESSED_MIN_RETRY_TIMOUT_MS = 300;
const DEFAULT_UNPROCESSED_MAX_RETRY_TIMOUT_MS = 1000;

async function retryUnprocessedItems(
  documentClient,
  unprocessedItems,
  batchNo,
  retryAttemptNo,
  retryOptions
) {
  if (retryAttemptNo > UNPROCESSED_RETRY_LIMIT) {
    throw new Error(
      `BatchWrite batch: ${batchNo} - returned UnprocessedItems after ${retryAttemptNo} attempts (${
        retryAttemptNo - 1
      } retries)`
    );
  }

  await new Promise((r) => {
    const randomTimeoutToAvoidThrottling = randomInteger(
      retryOptions.minMs,
      retryOptions.maxMs
    );

    setTimeout(r, randomTimeoutToAvoidThrottling);
  });

  const batchWriteCommand = new BatchWriteCommand({
    RequestItems: unprocessedItems
  });

  await execAndRetryBatchWrite(
    documentClient,
    batchWriteCommand,
    batchNo,
    retryAttemptNo,
    retryOptions
  );
}

async function execAndRetryBatchWrite(
  documentClient,
  batchWriteCommand,
  batchNo,
  retryCount,
  retryOptions
) {
  const res = await documentClient.send(batchWriteCommand);

  if (res?.UnprocessedItems && Object.keys(res?.UnprocessedItems).length > 0) {
    console.log(
      `UnprocessedItems, retrying, attempt ${retryCount + 1})...`,
      res.UnprocessedItems
    );
    await retryUnprocessedItems(
      documentClient,
      res.UnprocessedItems,
      batchNo,
      retryCount + 1,
      retryOptions
    );
  }

  return res;
}

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

function parseRetryOptions(retryTimeoutMinMs, retryTimeoutMaxMs) {
  return {
    minMs:
      retryTimeoutMinMs >= 0
        ? retryTimeoutMinMs
        : DEFAULT_UNPROCESSED_MIN_RETRY_TIMOUT_MS,
    maxMs:
      retryTimeoutMaxMs >= 0
        ? retryTimeoutMaxMs
        : DEFAULT_UNPROCESSED_MAX_RETRY_TIMOUT_MS
  };
}

async function batchWrite(
  documentClient,
  tableName,
  items,
  retryTimeoutMinMs,
  retryTimeoutMaxMs
) {
  const chunkedItems = chunk(items, MAX_ITEMS_PER_BATCH);

  const retryOptions = parseRetryOptions(retryTimeoutMinMs, retryTimeoutMaxMs);

  const runBatches = chunkedItems.map((batch, index) => {
    const batchWriteCommand = createBatchWriteCommand(tableName, batch);

    return execAndRetryBatchWrite(
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

module.exports = {
  batchWrite,
  MAX_ITEMS_PER_BATCH
};
