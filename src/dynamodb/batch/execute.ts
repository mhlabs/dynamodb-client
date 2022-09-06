import {
  BatchGetCommand,
  BatchGetCommandOutput,
  BatchWriteCommand,
  BatchWriteCommandOutput
} from '@aws-sdk/lib-dynamodb';
import { BaseInput, MhDynamoClient } from '../../..';

import { randomInteger } from '../../randomizer/random-integer';
import { constants } from './constants';
import { RetryOptions } from './retry-options';

export interface ExecuteInput<T> extends BaseInput {
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
  input: ExecuteInput<T>,
  response: BatchGetCommandOutput | BatchWriteCommandOutput
): Promise<T[]> {
  if (input.retryCount > constants.UNPROCESSED_ITEMS_RETRY_LIMIT) {
    const unprocessed = getUnprocessedPropertyNameFromResponse(response);

    throw new Error(
      `Batch: ${input.batchNo} - returned ${unprocessed} after ${
        input.retryCount
      } attempts (${input.retryCount - 1} retries)`
    );
  }

  const randomTimeoutToAvoidThrottling = randomInteger(
    input.retryOptions.minMs,
    input.retryOptions.maxMs
  );

  console.log(
    `UnprocessedItems, retrying after waiting ${randomTimeoutToAvoidThrottling} ms, attempt ${input.retryCount})...`
  );

  await new Promise((r) => {
    setTimeout(r, randomTimeoutToAvoidThrottling);
  });

  const command = createRetryCommandFromResponse(response);
  return await this.execute<T>({ ...input, batchCommand: command });
}

export async function execute<T>(
  this: MhDynamoClient,
  input: ExecuteInput<T>
): Promise<T[]> {
  let items: T[] = [...input.previousItems];

  const response: BatchGetCommandOutput | BatchWriteCommandOutput =
    await this.documentClient.send(input.batchCommand as any);

  if ('Responses' in response) {
    if (response.Responses && response.Responses[input.tableName]) {
      items.push(...(response.Responses[input.tableName] as T[]));
    }
  }

  if (!needsRetry(response)) return items;
  items = await this.retryUnprocessedItems<T>(
    { ...input, previousItems: items, retryCount: ++input.retryCount },
    response
  );

  return items;
}
