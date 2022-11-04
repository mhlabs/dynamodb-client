import {
  BatchGetCommand,
  BatchGetCommandOutput,
  BatchWriteCommand,
  BatchWriteCommandOutput,
  DynamoDBDocument
} from '@aws-sdk/lib-dynamodb';
import { BaseOptions } from '../../types';

import { randomInteger } from '../../randomizer/random-integer';
import { constants } from './constants';
import { RetryOptions } from './retry-options';

export interface ExecuteOptions<T> extends BaseOptions {
  batchCommand: BatchGetCommand | BatchWriteCommand;
  batchNo: number;
  retryCount: number;
  retryOptions: RetryOptions;
  delayMsBetweenCalls?: number;
  previousItems: T[];
}

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

export async function retryUnprocessedItems<T>(
  client: DynamoDBDocument,
  options: ExecuteOptions<T>,
  response: BatchGetCommandOutput | BatchWriteCommandOutput
): Promise<T[]> {
  if (options.retryCount > constants.UNPROCESSED_ITEMS_RETRY_LIMIT) {
    const unprocessed = getUnprocessedPropertyNameFromResponse(response);

    throw new Error(
      `Batch: ${options.batchNo} - returned ${unprocessed} after ${
        options.retryCount
      } attempts (${options.retryCount - 1} retries)`
    );
  }

  const randomTimeoutToAvoidThrottling = randomInteger(
    options.retryOptions.minMs,
    options.retryOptions.maxMs
  );

  console.log(
    `UnprocessedItems, retrying after waiting ${randomTimeoutToAvoidThrottling} ms, attempt ${options.retryCount})...`
  );

  await sleep(randomTimeoutToAvoidThrottling);

  const command = createRetryCommandFromResponse(response);
  return await execute<T>(client, { ...options, batchCommand: command });
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export async function execute<T>(
  client: DynamoDBDocument,
  options: ExecuteOptions<T>
): Promise<T[]> {
  let items: T[] = [...options.previousItems];

  const response: BatchGetCommandOutput | BatchWriteCommandOutput =
    await client.send(options.batchCommand as any);

  if ('Responses' in response) {
    if (response.Responses && response.Responses[options.tableName as string]) {
      items.push(...(response.Responses[options.tableName as string] as T[]));
    }
  }

  if (options.delayMsBetweenCalls) await sleep(options.delayMsBetweenCalls);

  if (!needsRetry(response)) return items;
  items = await retryUnprocessedItems<T>(
    client,
    { ...options, previousItems: items, retryCount: ++options.retryCount },
    response
  );

  return items;
}
