import { BatchWriteCommand } from '@aws-sdk/lib-dynamodb';

import { MhDynamoClient } from '../..';
import { chunk } from '../../array/chunk';
import { BaseOptions, BatchRetryOptions, MultiItemOptions } from '../../types';
import { ensureValidBatch } from '../../validation';
import { constants } from './constants';
import { filterUniqueKeys } from './duplicate-handling/filter';
import { execute } from './execute';
import { parseRetryOptions } from './retry-options';

export interface BatchRemoveOptions
  extends BaseOptions,
    BatchRetryOptions,
    Omit<MultiItemOptions, 'items'> {
  delayMsBetweenCalls?: number;
}

const createBatchDeleteCommand = (
  tableName: string,
  batch: Record<string, any>[]
): BatchWriteCommand => {
  const deleteRequests = batch.map((key) => ({
    DeleteRequest: {
      Key: key
    }
  }));
  return new BatchWriteCommand({
    RequestItems: {
      [tableName]: deleteRequests
    }
  });
};

export async function batchRemove(
  this: MhDynamoClient,
  options: BatchRemoveOptions
): Promise<boolean> {
  options = this.mergeWithGlobalOptions(options);
  ensureValidBatch(options, options.keys);

  if (!options.keys.length) return true;

  const uniqueKeys = filterUniqueKeys(options.keys);
  const chunkedItems = chunk(uniqueKeys, constants.MAX_ITEMS_PER_BATCH_WRITE);

  const retryOptions = parseRetryOptions(
    options.retryTimeoutMinMs,
    options.retryTimeoutMaxMs
  );

  const runBatches = chunkedItems.map((batch, index) => {
    const batchWriteCommand = createBatchDeleteCommand(
      options.tableName as string,
      batch
    );

    return execute(this.documentClient, {
      tableName: options.tableName,
      batchCommand: batchWriteCommand,
      batchNo: index + 1,
      retryCount: 0,
      retryOptions,
      previousItems: [],
      delayMsBetweenCalls: options.delayMsBetweenCalls
    });
  });

  await Promise.all(runBatches);

  return true;
}
