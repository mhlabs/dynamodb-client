import {
  DynamoDB,
  DynamoDBClient,
  DynamoDBClientConfig
} from '@aws-sdk/client-dynamodb';
import {
  DeleteCommandInput,
  DynamoDBDocument,
  GetCommandInput,
  PutCommandInput,
  ScanCommandInput,
  TranslateConfig
} from '@aws-sdk/lib-dynamodb';

import { batchGet } from './src/dynamodb/batch/get';
import { batchRemove } from './src/dynamodb/batch/remove';
import { batchWrite } from './src/dynamodb/batch/write';
import { getItem } from './src/dynamodb/get-item';
import { putItem } from './src/dynamodb/put-item';
import { remove } from './src/dynamodb/remove';
import { query } from './src/dynamodb/query';
import { scan } from './src/dynamodb/scan';
import { DuplicateOptions } from './src/dynamodb/batch/duplicate-handling/filter';

export interface MhDynamoClient {
  batchGet: <T>(
    tableName: string,
    keys: Record<string, any>[],
    options?: Record<string, any>,
    retryTimeoutMinMs?: number,
    retryTimeoutMaxMs?: number
  ) => Promise<T[]>;
  batchRemove: (
    tableName: string,
    keys: Record<string, any>[],
    retryTimeoutMinMs?: number,
    retryTimeoutMaxMs?: number
  ) => Promise<boolean>;
  batchWrite: (
    tableName: string,
    keys: Record<string, any>[],
    options?: DuplicateOptions,
    retryTimeoutMinMs?: number,
    retryTimeoutMaxMs?: number
  ) => Promise<boolean>;
  getItem: <T>(
    tableName: string,
    key: Record<string, any>,
    options?: GetCommandInput
  ) => Promise<T | null>;
  putItem: (
    tableName: string,
    item: Record<string, any>,
    options?: PutCommandInput
  ) => Promise<boolean>;
  query: <T>(
    tableName: string,
    keyCondition: Record<string, any>
  ) => Promise<T[]>;
  queryByIndex: <T>(
    tableName: string,
    keyCondition: Record<string, any>,
    indexName?: string
  ) => Promise<T[]>;
  remove: (
    tableName: string,
    key: Record<string, any>,
    options?: DeleteCommandInput
  ) => Promise<boolean>;
  scan: <T>(tableName: string, options?: ScanCommandInput) => Promise<T[]>;
}

const createDynamoClient = (documentClient): MhDynamoClient => {
  return {
    batchGet: (
      tableName,
      keys,
      options,
      retryTimeoutMinMs,
      retryTimeoutMaxMs
    ) =>
      batchGet(
        documentClient,
        tableName,
        keys,
        options,
        retryTimeoutMinMs,
        retryTimeoutMaxMs
      ),
    batchRemove: (tableName, keys, retryTimeoutMinMs, retryTimeoutMaxMs) =>
      batchRemove(
        documentClient,
        tableName,
        keys,
        retryTimeoutMinMs,
        retryTimeoutMaxMs
      ),
    batchWrite: (
      tableName,
      items,
      options,
      retryTimeoutMinMs,
      retryTimeoutMaxMs
    ) =>
      batchWrite(
        documentClient,
        tableName,
        items,
        options,
        retryTimeoutMinMs,
        retryTimeoutMaxMs
      ),
    getItem: (tableName, key, options) =>
      getItem(documentClient, tableName, key, options),
    putItem: (tableName, item, options) =>
      putItem(documentClient, tableName, item, options),
    query: (tableName, keyCondition) =>
      query(documentClient, tableName, keyCondition),
    queryByIndex: (tableName, keyCondition, indexName) =>
      query(documentClient, tableName, keyCondition, true, indexName),
    remove: (tableName, key, options) =>
      remove(documentClient, tableName, key, options),
    scan: (tableName, options) => scan(documentClient, tableName, options)
  };
};

export const init = (
  dynamoDbClientConfig: DynamoDBClientConfig,
  translateConfig?: TranslateConfig
): MhDynamoClient => {
  const client = new DynamoDB(dynamoDbClientConfig);

  const translateOptions = { ...translateConfig };

  if (!translateOptions.marshallOptions) {
    translateOptions.marshallOptions = {
      removeUndefinedValues: true
    };
  }

  const documentClient = DynamoDBDocument.from(client, translateOptions);
  return createDynamoClient(documentClient);
};

export const initWithClient = (
  client: DynamoDBClient,
  translateConfig?: TranslateConfig
): MhDynamoClient => {
  const translateOptions = { ...translateConfig };

  if (!translateOptions.marshallOptions) {
    translateOptions.marshallOptions = {
      removeUndefinedValues: true
    };
  }

  const documentClient = DynamoDBDocument.from(client, translateOptions);
  return createDynamoClient(documentClient);
};
