import { BatchWriteCommand } from '@aws-sdk/lib-dynamodb';

import { MhDynamoClient } from '../../..';
import {
  BaseSaveOptions,
  BatchRetryOptions,
  MultiItemOptions
} from '../../../types';
import { chunk } from '../../array/chunk';
import { constants } from './constants';
import {
  defaultDuplicateOptions,
  DuplicateOptions,
  filterUniqueObjects
} from './duplicate-handling/filter';
import { parseRetryOptions } from './retry-options';

export interface BatchWriteOptions
  extends BaseSaveOptions,
    BatchRetryOptions,
    Omit<MultiItemOptions, 'keys'> {
  options?: DuplicateOptions;
}

const createBatchWriteCommand = (
  tableName: string,
  batch: Record<string, any>[]
): BatchWriteCommand => {
  const putRequests = batch.map((item) => ({
    PutRequest: {
      Item: item
    }
  }));
  return new BatchWriteCommand({
    RequestItems: {
      [tableName]: putRequests
    }
  });
};

export async function batchWrite(
  this: MhDynamoClient,
  options: BatchWriteOptions
): Promise<boolean> {
  options = this.mergeWithGlobalOptions(options);
  this.ensureValidBatchWrite(options, options.items);
  if (!options.items.length) return true;

  const commandOptions = { ...defaultDuplicateOptions, ...options.options };

  const uniqueItems = filterUniqueObjects(options.items, commandOptions);
  const chunkedItems = chunk(uniqueItems, constants.MAX_ITEMS_PER_BATCH_WRITE);

  const retryOptions = parseRetryOptions(
    options.retryTimeoutMinMs,
    options.retryTimeoutMaxMs
  );

  const runBatches = chunkedItems.map((batch, index) => {
    const batchWriteCommand = createBatchWriteCommand(input.tableName, batch);

    return this.execute({
      tableName: options.tableName,
      batchCommand: batchWriteCommand,
      batchNo: index + 1,
      retryCount: 0,
      retryOptions,
      previousItems: []
    });
  });

  await Promise.all(runBatches);

  return true;
}
