import { BatchWriteCommand } from '@aws-sdk/lib-dynamodb';

import { MhDynamoClient } from '../..';
import { chunk } from '../../array/chunk';
import { enrichInputs } from '../../enrich';
import {
  BaseSaveOptions,
  BatchRetryOptions,
  MultiItemOptions
} from '../../types';
import { ensureValidBatchWrite } from '../../validation';
import { constants } from './constants';
import {
  defaultDuplicateOptions,
  DuplicateOptions,
  filterUniqueObjects
} from './duplicate-handling/filter';
import { execute } from './execute';
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
  ensureValidBatchWrite(options, options.items);
  if (!options.items.length) return true;

  const commandOptions = { ...defaultDuplicateOptions, ...options.options };

  const uniqueItems = filterUniqueObjects(options.items, commandOptions);
  const chunkedItems = chunk(uniqueItems, constants.MAX_ITEMS_PER_BATCH_WRITE);

  const retryOptions = parseRetryOptions(
    options.retryTimeoutMinMs,
    options.retryTimeoutMaxMs
  );

  const runBatches = chunkedItems.map((batch, index) => {
    batch = enrichInputs(batch, options);
    const batchWriteCommand = createBatchWriteCommand(
      options.tableName as string,
      batch
    );

    return execute(this.documentClient, {
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
