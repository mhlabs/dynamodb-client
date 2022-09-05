import { BatchWriteCommand, DynamoDBDocument } from '@aws-sdk/lib-dynamodb';

import { chunk } from '../../array/chunk';
import { constants } from './constants';
import { execute } from './execute';
import { parseRetryOptions } from './retry-options';
import {
  filterUniqueObjects,
  defaultDuplicateOptions,
  DuplicateOptions
} from './duplicate-handling/filter';
import { isMultidimensional } from '../../array/isMultidimensional';

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

const ensureValidParameters = (
  documentClient: DynamoDBDocument,
  tableName: string,
  items: Record<string, any>[]
) => {
  if (!documentClient) throw new Error('documentClient is required.');
  if (!tableName) throw new Error('Table name is required.');
  if (!items) throw new Error('Item list is required.');
  if (isMultidimensional(items))
    throw new Error("Item list can't contain arrays (be multidimensional).");
};

export const batchWrite = async (
  documentClient: DynamoDBDocument,
  tableName: string,
  items: Record<string, any>[],
  options?: DuplicateOptions,
  retryTimeoutMinMs?: number,
  retryTimeoutMaxMs?: number
): Promise<boolean> => {
  ensureValidParameters(documentClient, tableName, items);

  if (!items.length) return true;

  const commandOptions = { ...defaultDuplicateOptions, ...options };

  const uniqueItems = filterUniqueObjects(items, commandOptions);
  const chunkedItems = chunk(uniqueItems, constants.MAX_ITEMS_PER_BATCH_WRITE);

  const retryOptions = parseRetryOptions(retryTimeoutMinMs, retryTimeoutMaxMs);

  const runBatches = chunkedItems.map((batch, index) => {
    const batchWriteCommand = createBatchWriteCommand(tableName, batch);

    return execute(
      documentClient,
      tableName,
      batchWriteCommand,
      index + 1,
      0,
      retryOptions
    );
  });

  await Promise.all(runBatches);

  return true;
};
