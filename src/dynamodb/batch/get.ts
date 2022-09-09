import { BatchGetCommand, BatchGetCommandInput } from '@aws-sdk/lib-dynamodb';

import { chunk } from '../../array/chunk';
import { constants } from './constants';

import { MhDynamoClient } from '../..';
import { sanitizeOutputs } from '../../sanitize';
import {
  BaseFetchOptions,
  BatchRetryOptions,
  MultiItemOptions
} from '../../types';
import { ensureValidBatch } from '../../validation';
import { filterUniqueKeys } from './duplicate-handling/filter';
import { execute } from './execute';
import { parseRetryOptions } from './retry-options';

export interface BatchGetOptions
  extends BaseFetchOptions,
    BatchRetryOptions,
    Omit<MultiItemOptions, 'items'> {
  batchOptions?: Record<string, any>;
}

const createBatchGetCommand = (
  tableName: string,
  batch: Record<string, any>[],
  batchOptions?: Record<string, any>
) => {
  const input: BatchGetCommandInput = {
    RequestItems: {
      [tableName]: {
        Keys: batch,
        ...batchOptions
      }
    }
  };

  return new BatchGetCommand(input);
};

export async function batchGet<T>(
  this: MhDynamoClient,
  options: BatchGetOptions
): Promise<T[]> {
  options = this.mergeWithGlobalOptions(options);
  ensureValidBatch(options, options.keys);

  if (!options.keys.length) return [];

  const uniqueKeys = filterUniqueKeys(options.keys);
  const chunkedItems = chunk(uniqueKeys, constants.MAX_KEYS_PER_BATCH_GET);

  const retryOptions = parseRetryOptions(
    options.retryTimeoutMinMs,
    options.retryTimeoutMaxMs
  );

  const runBatches = chunkedItems.map((batch, index) => {
    const batchGetCommand = createBatchGetCommand(
      options.tableName as string,
      batch,
      options.batchOptions
    );
    return execute<T>(this.documentClient, {
      tableName: options.tableName,
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

  return sanitizeOutputs(items, options);
}
