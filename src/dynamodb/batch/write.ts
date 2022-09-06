import { BatchWriteCommand } from '@aws-sdk/lib-dynamodb';

import {
  BaseInput,
  BatchRetryInput,
  MhDynamoClient,
  MultiItemInput
} from '../../..';
import { chunk } from '../../array/chunk';
import { constants } from './constants';
import {
  defaultDuplicateOptions,
  DuplicateOptions,
  filterUniqueObjects
} from './duplicate-handling/filter';
import { parseRetryOptions } from './retry-options';

export interface BatchWriteInput
  extends BaseInput,
    BatchRetryInput,
    Omit<MultiItemInput, 'keys'> {
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
  input: BatchWriteInput
): Promise<boolean> {
  this.ensureValidBatchWrite(input.tableName, input.items);
  if (!input.items.length) return true;

  const commandOptions = { ...defaultDuplicateOptions, ...input.options };

  const uniqueItems = filterUniqueObjects(input.items, commandOptions);
  const chunkedItems = chunk(uniqueItems, constants.MAX_ITEMS_PER_BATCH_WRITE);

  const retryOptions = parseRetryOptions(
    input.retryTimeoutMinMs,
    input.retryTimeoutMaxMs
  );

  const runBatches = chunkedItems.map((batch, index) => {
    const batchWriteCommand = createBatchWriteCommand(input.tableName, batch);

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
