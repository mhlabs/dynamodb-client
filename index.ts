import {
  DynamoDB,
  DynamoDBClient,
  DynamoDBClientConfig
} from '@aws-sdk/client-dynamodb';
import { DynamoDBDocument, TranslateConfig } from '@aws-sdk/lib-dynamodb';

import { isMultidimensional } from './src/array/isMultidimensional';
import { execute, retryUnprocessedItems } from './src/dynamodb/batch/execute';
import { batchGet } from './src/dynamodb/batch/get';
import { batchRemove } from './src/dynamodb/batch/remove';
import { batchWrite } from './src/dynamodb/batch/write';
import { getItem } from './src/dynamodb/get-item';
import { putItem } from './src/dynamodb/put-item';
import { query, queryByIndex } from './src/dynamodb/query';
import { remove } from './src/dynamodb/remove';
import { scan } from './src/dynamodb/scan';

export interface MhDynamoClientOptions {
  translateConfig?: TranslateConfig;
  tableName?: string;
}

export interface BaseInput {
  tableName: string;
}

export interface BatchRetryInput {
  retryTimeoutMinMs?: number;
  retryTimeoutMaxMs?: number;
}

export interface SingleItemInput {
  key: Record<string, any>;
  item: Record<string, any>;
}

export interface MultiItemInput {
  items: Record<string, any>[];
  keys: Record<string, any>[];
}

export class MhDynamoClient {
  private options: MhDynamoClientOptions;
  protected documentClient: DynamoDBDocument;

  static fromClient(client: DynamoDBClient, options?: MhDynamoClientOptions) {
    return new MhDynamoClient(client, options);
  }

  static fromConfig(
    dynamoDbClientConfig: DynamoDBClientConfig,
    options?: MhDynamoClientOptions
  ) {
    const client = new DynamoDB(dynamoDbClientConfig);
    return new MhDynamoClient(client, options);
  }

  static fromDocumentClient(
    dynamoDbDocumentClient: DynamoDBDocument,
    options?: MhDynamoClientOptions
  ) {
    return new MhDynamoClient(dynamoDbDocumentClient, options);
  }

  /**
   * Cannot be initiated. Use MhDynamoClient.fromClient() or MhDynamoClient.fromConfig() instead.
   */
  private constructor(
    client: DynamoDBClient | DynamoDBDocument,
    options?: MhDynamoClientOptions
  ) {
    this.options = options || {};

    if (client as DynamoDBDocument) {
      this.documentClient = client as DynamoDBDocument;
      return this;
    }

    const translateOptions = { ...this.options.translateConfig };
    if (!translateOptions?.marshallOptions) {
      translateOptions.marshallOptions = {
        removeUndefinedValues: true
      };
    }

    this.options.translateConfig = translateOptions;
    this.documentClient = DynamoDBDocument.from(
      client as DynamoDBClient,
      translateOptions
    );
  }

  protected ensureValidBase(tableName: string) {
    if (!this.documentClient) throw new Error('documentClient is required.');
    if (!tableName) throw new Error('tableName is required.');
  }

  protected ensureValid(
    tableName: string,
    object: Record<string, any>,
    objectName = 'object'
  ) {
    this.ensureValidBase(tableName);

    if (!object) throw new Error('object is required.');
    if (typeof object !== 'object')
      throw new Error(`${objectName} should be an object.`);
  }

  protected ensureValidQuery(
    tableName: string,
    object: Record<string, any>,
    indexName: string
  ) {
    this.ensureValid(tableName, object, 'keyCondition');
    if (!indexName) throw new Error('indexName is required.');
  }

  protected ensureValidBatchWrite(
    tableName: string,
    items: Record<string, any>[]
  ) {
    this.ensureValidBase(tableName);

    if (!items) throw new Error('Item list is required.');
    if (isMultidimensional(items)) {
      throw new Error("Item list can't contain arrays (be multidimensional).");
    }
  }

  protected ensureValidBatch(tableName: string, keys: Record<string, any>[]) {
    this.ensureValidBase(tableName);

    if (!keys) throw new Error('Key list is required.');
    if (!keys.every((key) => typeof key === 'object')) {
      throw new Error('Keys must be objects.');
    }
  }

  public remove = remove;
  public batchRemove = batchRemove;
  public putItem = putItem;
  public batchWrite = batchWrite;
  public getItem = getItem;
  public batchGet = batchGet;
  public scan = scan;
  public query = query;
  public queryByIndex = queryByIndex;

  public execute = execute;
  protected retryUnprocessedItems = retryUnprocessedItems;
}
