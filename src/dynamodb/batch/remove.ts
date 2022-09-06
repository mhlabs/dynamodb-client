import { BatchWriteCommand } from '@aws-sdk/lib-dynamodb';

import {
  BaseInput,
  BatchRetryInput,
  MhDynamoClient,
  MultiItemInput
} from '../../..';
import { chunk } from '../../array/chunk';
import { constants } from './constants';
import { filterUniqueKeys } from './duplicate-handling/filter';
import { parseRetryOptions } from './retry-options';

export interface BatchRemoveInput
  extends BaseInput,
    BatchRetryInput,
    Omit<MultiItemInput, 'items'> {}

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
  input: BatchRemoveInput
): Promise<boolean> {
  this.ensureValidBatch(input.tableName, input.keys);

  if (!input.keys.length) return true;

  const uniqueKeys = filterUniqueKeys(input.keys);
  const chunkedItems = chunk(uniqueKeys, constants.MAX_ITEMS_PER_BATCH_WRITE);

  const retryOptions = parseRetryOptions(
    input.retryTimeoutMinMs,
    input.retryTimeoutMaxMs
  );

  const runBatches = chunkedItems.map((batch, index) => {
    const batchWriteCommand = createBatchDeleteCommand(input.tableName, batch);

    return this.execute({
      tableName: input.tableName,
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
