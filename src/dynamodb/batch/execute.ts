import {
  BatchGetCommand,
  BatchGetCommandOutput,
  BatchWriteCommand,
  BatchWriteCommandOutput
} from '@aws-sdk/lib-dynamodb';
import { MhDynamoClient } from '../..';
import { BaseOptions } from '../../types';

import { randomInteger } from '../../randomizer/random-integer';
import { constants } from './constants';
import { RetryOptions } from './retry-options';

export interface ExecuteOptions<T> extends BaseOptions {
  batchCommand: BatchGetCommand | BatchWriteCommand;
  batchNo: number;
  retryCount: number;
  retryOptions: RetryOptions;
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
  this: MhDynamoClient,
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

  await new Promise((r) => {
    setTimeout(r, randomTimeoutToAvoidThrottling);
  });

  const command = createRetryCommandFromResponse(response);
  return await this.execute<T>({ ...options, batchCommand: command });
}

export async function execute<T>(
  this: MhDynamoClient,
  options: ExecuteOptions<T>
): Promise<T[]> {
  let items: T[] = [...options.previousItems];

  const response: BatchGetCommandOutput | BatchWriteCommandOutput =
    await this.documentClient.send(options.batchCommand as any);

  if ('Responses' in response) {
    if (response.Responses && response.Responses[options.tableName as string]) {
      items.push(...(response.Responses[options.tableName as string] as T[]));
    }
  }

  if (!needsRetry(response)) return items;
  items = await this.retryUnprocessedItems<T>(
    { ...options, previousItems: items, retryCount: ++options.retryCount },
    response
  );

  return items;
}
