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
  const hasUnprocessedKeys =
    'UnprocessedKeys' in response &&
    !!response?.UnprocessedKeys &&
    Object.keys(response.UnprocessedKeys).length > 0;

  const hasUnprocessedItems =
    'UnprocessedItems' in response &&
    !!response?.UnprocessedItems &&
    Object.keys(response.UnprocessedItems).length > 0;

  return hasUnprocessedKeys || hasUnprocessedItems;
};

const getUnprocessedPropertyNameFromResponse = (
  response: BatchGetCommandOutput | BatchWriteCommandOutput
) => {
  if ('UnprocessedKeys' in response) return 'UnprocessedKeys';
  if ('UnprocessedItems' in response) return 'UnprocessedItems';
  return 'Unsupported batch command response when getting property name from response';
};

const createRetryCommandFromResponse = (
  response: BatchGetCommandOutput | BatchWriteCommandOutput
) => {
  if ('UnprocessedKeys' in response && response?.UnprocessedKeys) {
    return new BatchGetCommand({ RequestItems: response.UnprocessedKeys });
  } else if ('UnprocessedItems' in response && response?.UnprocessedItems) {
    return new BatchWriteCommand({ RequestItems: response.UnprocessedItems });
  }

  throw new Error(
    'Unsupported batch command response when creating retry command'
  );
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
  let items: T[] = [...previousItems];

  const response: BatchGetCommandOutput | BatchWriteCommandOutput =
    await documentClient.send(batchCommand as any);

  if ('Responses' in response) {
    if (response.Responses && response.Responses[tableName]) {
      items.push(...(response.Responses[tableName] as T[]));
    }
  }

  if (!needsRetry(response)) return items;
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
