import {
  BatchGetCommand,
  BatchGetCommandOutput,
  BatchWriteCommand,
  BatchWriteCommandOutput,
  DynamoDBDocument
} from '@aws-sdk/lib-dynamodb';

import { randomInteger } from '../../randomizer/random-integer';
import { constants } from './constants';
import { RetryOptions } from './retry-options';

const needsRetry = (
  response: BatchGetCommandOutput | BatchWriteCommandOutput
): boolean => {
  if (response as BatchGetCommandOutput) {
    const getResponse = response as BatchGetCommandOutput;
    return (
      (getResponse?.UnprocessedKeys || false) &&
      Object.keys(getResponse?.UnprocessedKeys).length > 0
    );
  }

  if (response as BatchWriteCommandOutput) {
    const writeResponse = response as BatchWriteCommandOutput;
    return (
      (writeResponse?.UnprocessedItems || false) &&
      Object.keys(writeResponse?.UnprocessedItems).length > 0
    );
  }

  throw new Error('Unsupported batch command response');
};

const getUnprocessedPropertyNameFromResponse = (
  response: BatchGetCommandOutput | BatchWriteCommandOutput
) => {
  if (response as BatchGetCommandOutput) return 'UnprocessedKeys';
  if (response as BatchWriteCommandOutput) return 'UnprocessedItems';
  return 'Unsupported batch command response';
};

const createRetryCommandFromResponse = (
  response: BatchGetCommandOutput | BatchWriteCommandOutput
) => {
  if (response as BatchGetCommandOutput) {
    const getResponse = response as BatchGetCommandOutput;
    if (!getResponse.UnprocessedKeys) {
      throw new Error('Retry called without unprocessed keys');
    }

    return new BatchGetCommand({ RequestItems: getResponse.UnprocessedKeys });
  } else if (response as BatchWriteCommandOutput) {
    const writeResponse = response as BatchWriteCommandOutput;
    if (!writeResponse.UnprocessedItems) {
      throw new Error('Retry called without unprocessed items');
    }

    return new BatchWriteCommand({
      RequestItems: writeResponse.UnprocessedItems
    });
  }

  throw new Error('Unsupported batch command response');
};

const retryUnprocessedItems = async <T>(
  documentClient: DynamoDBDocument,
  tableName: string,
  response: BatchGetCommandOutput | BatchWriteCommandOutput,
  batchNo: number,
  retryAttemptNo: number,
  retryOptions: RetryOptions,
  previousItems: T[]
): Promise<T[]> => {
  if (retryAttemptNo > constants.UNPROCESSED_ITEMS_RETRY_LIMIT) {
    const unprocessed = getUnprocessedPropertyNameFromResponse(response);

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

  const command = createRetryCommandFromResponse(response);
  return await execute<T>(
    documentClient,
    tableName,
    command,
    batchNo,
    retryAttemptNo,
    retryOptions,
    previousItems
  );
};

export const execute = async <T>(
  documentClient: DynamoDBDocument,
  tableName: string,
  batchCommand: BatchGetCommand | BatchWriteCommand,
  batchNo: number,
  retryCount: number,
  retryOptions: RetryOptions,
  previousItems: T[] = []
): Promise<T[]> => {
  console.log('Execute called with items', { previousItems });
  let items: T[] = [...previousItems];
  let response: BatchGetCommandOutput | BatchWriteCommandOutput;

  if (batchCommand as BatchGetCommand) {
    const batchGet = batchCommand as BatchGetCommand;
    const getResponse = await documentClient.send(batchGet);

    if (getResponse.Responses && getResponse.Responses[tableName]) {
      items.push(...(getResponse.Responses[tableName] as T[]));
    }

    response = getResponse;
  } else if (batchCommand as BatchWriteCommand) {
    const batchWrite = batchCommand as BatchWriteCommand;

    response = await documentClient.send(batchWrite);
  } else {
    throw new Error('Unsupported batch command');
  }

  if (!needsRetry(response)) return items;

  console.log('Needs retry');

  items = await retryUnprocessedItems<T>(
    documentClient,
    tableName,
    response,
    batchNo,
    retryCount + 1,
    retryOptions,
    items
  );

  return items;
};
