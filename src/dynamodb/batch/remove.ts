import { BatchWriteCommand, DynamoDBDocument } from '@aws-sdk/lib-dynamodb';

import { chunk } from '../../array/chunk';
import { constants } from './constants';
import { execute } from './execute';
import { parseRetryOptions } from './retry-options';
import { filterUniqueKeys } from './duplicate-handling/filter';

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

const ensureValidParameters = (
  documentClient: DynamoDBDocument,
  tableName: string,
  keys: Record<string, any>[]
) => {
  if (!documentClient) throw new Error('documentClient is required.');
  if (!tableName) throw new Error('tableName is required.');
  if (!keys) throw new Error('Key list is required.');

  if (!keys.every((key) => typeof key === 'object'))
    throw new Error('Keys must be objects.');
};

export const batchRemove = async (
  documentClient: DynamoDBDocument,
  tableName: string,
  keys: Record<string, any>[],
  retryTimeoutMinMs?: number,
  retryTimeoutMaxMs?: number
): Promise<boolean> => {
  ensureValidParameters(documentClient, tableName, keys);

  if (!keys.length) return true;

  const uniqueKeys = filterUniqueKeys(keys);
  const chunkedItems = chunk(uniqueKeys, constants.MAX_ITEMS_PER_BATCH_WRITE);

  const retryOptions = parseRetryOptions(retryTimeoutMinMs, retryTimeoutMaxMs);

  const runBatches = chunkedItems.map((batch, index) => {
    const batchWriteCommand = createBatchDeleteCommand(tableName, batch);

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
