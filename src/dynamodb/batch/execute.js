const { BatchWriteCommand } = require('@aws-sdk/lib-dynamodb');

const { randomInteger } = require('../../randomizer/random-integer');
const constants = require('./constants');

async function retryUnprocessedItems(
  documentClient,
  unprocessedItems,
  batchNo,
  retryAttemptNo,
  retryOptions
) {
  if (retryAttemptNo > constants.UNPROCESSED_RETRY_LIMIT) {
    throw new Error(
      `BatchWrite batch: ${batchNo} - returned UnprocessedItems after ${retryAttemptNo} attempts (${
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

  const batchWriteCommand = new BatchWriteCommand({
    RequestItems: unprocessedItems
  });

  await execute(
    documentClient,
    batchWriteCommand,
    batchNo,
    retryAttemptNo,
    retryOptions
  );
}

async function execute(
  documentClient,
  batchWriteCommand,
  batchNo,
  retryCount,
  retryOptions
) {
  const res = await documentClient.send(batchWriteCommand);

  if (res?.UnprocessedItems && Object.keys(res?.UnprocessedItems).length > 0) {
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

module.exports = execute;
