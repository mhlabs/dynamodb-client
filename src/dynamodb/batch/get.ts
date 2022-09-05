import {
  BatchGetCommand,
  BatchGetCommandInput,
  DynamoDBDocument
} from '@aws-sdk/lib-dynamodb';

import { chunk } from '../../array/chunk';
import { constants } from './constants';

import { execute } from './execute';
import { parseRetryOptions } from './retry-options';
import { filterUniqueKeys } from './duplicate-handling/filter';

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

const ensureValidParameters = (
  documentClient: DynamoDBDocument,
  tableName: string,
  keys: Record<string, any>[]
) => {
  if (!documentClient) throw new Error('documentClient is required.');
  if (!tableName) throw new Error('tableName is required.');
  if (!keys) throw new Error('Key list is required.');
  if (!keys.every((key) => typeof key === 'object'))
    throw new Error('All keys should be objects.');
};

export const batchGet = async <T>(
  documentClient: DynamoDBDocument,
  tableName: string,
  keys: Record<string, any>[],
  options?: Record<string, any>,
  retryTimeoutMinMs?: number,
  retryTimeoutMaxMs?: number
): Promise<T[]> => {
  ensureValidParameters(documentClient, tableName, keys);

  if (!keys.length) return [];

  const uniqueKeys = filterUniqueKeys(keys);
  const chunkedItems = chunk(uniqueKeys, constants.MAX_KEYS_PER_BATCH_GET);

  const retryOptions = parseRetryOptions(retryTimeoutMinMs, retryTimeoutMaxMs);

  const runBatches = chunkedItems.map((batch, index) => {
    const batchGetCommand = createBatchGetCommand(tableName, batch, options);
    return execute<T>(
      documentClient,
      tableName,
      batchGetCommand,
      index + 1,
      0,
      retryOptions
    );
  });

  const responses = await Promise.all(runBatches);

  const items: T[] = [];
  responses.forEach((response) => items.push(...response));

  return items;
};
