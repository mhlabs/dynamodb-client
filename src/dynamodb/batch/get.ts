import { BatchGetCommand, BatchGetCommandInput } from '@aws-sdk/lib-dynamodb';

import { chunk } from '../../array/chunk';
import { constants } from './constants';

import {
  BaseInput,
  BatchRetryInput,
  MhDynamoClient,
  MultiItemInput
} from '../../..';
import { filterUniqueKeys } from './duplicate-handling/filter';
import { parseRetryOptions } from './retry-options';

export interface BatchGetInput
  extends BaseInput,
    BatchRetryInput,
    Omit<MultiItemInput, 'items'> {
  options?: Record<string, any>;
}

const createBatchGetCommand = (
  tableName: string,
  batch: Record<string, any>[],
  options?: Record<string, any>
) => {
  const input: BatchGetCommandInput = {
    RequestItems: {
      [tableName]: {
        Keys: batch,
        ...options
      }
    }
  };

  return new BatchGetCommand(input);
};

export async function batchGet<T>(
  this: MhDynamoClient,
  input: BatchGetInput
): Promise<T[]> {
  this.ensureValidBatch(input.tableName, input.keys);

  if (!input.keys.length) return [];

  const uniqueKeys = filterUniqueKeys(input.keys);
  const chunkedItems = chunk(uniqueKeys, constants.MAX_KEYS_PER_BATCH_GET);

  const retryOptions = parseRetryOptions(
    input.retryTimeoutMinMs,
    input.retryTimeoutMaxMs
  );

  const runBatches = chunkedItems.map((batch, index) => {
    const batchGetCommand = createBatchGetCommand(
      input.tableName,
      batch,
      input.options
    );
    return this.execute<T>({
      tableName: input.tableName,
      batchCommand: batchGetCommand,
      batchNo: index + 1,
      retryCount: 0,
      retryOptions,
      previousItems: []
    });
  });

  const responses = await Promise.all(runBatches);

  const items: T[] = [];
  responses.forEach((response) => items.push(...response));

  return items;
}
