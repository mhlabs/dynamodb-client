import {
  DynamoDB,
  DynamoDBClient,
  DynamoDBClientConfig
} from '@aws-sdk/client-dynamodb';
import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb';
import { batchGet } from './dynamodb/batch/get';

import { batchRemove } from './dynamodb/batch/remove';
import { batchWrite } from './dynamodb/batch/write';
import { getItem } from './dynamodb/get-item';
import { putItem } from './dynamodb/put-item';
import { query, queryByIndex } from './dynamodb/query';
import { remove } from './dynamodb/remove';
import { scan } from './dynamodb/scan';
import {
  BaseFetchOptions,
  BaseOptions,
  BaseSaveOptions,
  MhDynamoClientOptions
} from './types';

export class MhDynamoClient {
  private globalOptions: MhDynamoClientOptions;
  protected documentClient: DynamoDBDocument;

  private defaultOptions: MhDynamoClientOptions = {
    injectXrayTrace: true,
    extractXrayTrace: true,
    injectLastModified: true,
    extractLastModified: true,
  };

  static fromClient(client: DynamoDBClient, options?: MhDynamoClientOptions) {
    return new MhDynamoClient(client, undefined, options);
  }

  static fromConfig(
    dynamoDbClientConfig?: DynamoDBClientConfig,
    options?: MhDynamoClientOptions
  ) {
    const client = new DynamoDB(dynamoDbClientConfig || {});
    return new MhDynamoClient(client, undefined, options);
  }

  static fromDocumentClient(
    dynamoDbDocumentClient: DynamoDBDocument,
    options?: MhDynamoClientOptions
  ) {
    return new MhDynamoClient(undefined, dynamoDbDocumentClient, options);
  }

  /**
   * Cannot be initiated. Use either of the following instead:
   *
   * - MhDynamoClient.fromConfig()
   * - MhDynamoClient.fromClient()
   * - MhDynamoClient.fromDocumentClient()
   */
  private constructor(
    client?: DynamoDBClient,
    documentClient?: DynamoDBDocument,
    options?: MhDynamoClientOptions
  ) {
    this.globalOptions = {
      ...this.defaultOptions,
      ...options
    };

    if (documentClient) {
      this.documentClient = documentClient;
      return this;
    }

    if (!client)
      throw new Error(
        'Cannot instantiate without either client or document client set'
      );

    const translateOptions = { ...this.globalOptions.translateConfig };
    if (!translateOptions?.marshallOptions) {
      translateOptions.marshallOptions = {
        removeUndefinedValues: true
      };
    }

    this.globalOptions.translateConfig = translateOptions;
    this.documentClient = DynamoDBDocument.from(client, translateOptions);
  }

  // Public methods
  public remove = remove;
  public batchRemove = batchRemove;
  public putItem = putItem;
  public batchWrite = batchWrite;
  public getItem = getItem;
  public batchGet = batchGet;
  public scan = scan;
  public query = query;
  public queryByIndex = queryByIndex;

  // Helpers
  protected mergeWithGlobalOptions<T>(localOptions: T): T {
    const options = { ...localOptions };
    if (options as BaseOptions) {
      if (!(options as BaseOptions).tableName) {
        (options as BaseOptions).tableName = this.globalOptions.tableName;
      }
    }
    if (options as BaseFetchOptions) {
      if ((options as BaseFetchOptions).extractXrayTrace === undefined) {
        (options as BaseFetchOptions).extractXrayTrace =
            this.globalOptions.extractXrayTrace;
      }
      if ((options as BaseFetchOptions).extractLastModified === undefined) {
        (options as BaseFetchOptions).extractLastModified =
            this.globalOptions.extractLastModified;
      }
    }
    if (options as BaseSaveOptions) {
      if ((options as BaseSaveOptions).injectXrayTrace === undefined) {
        (options as BaseSaveOptions).injectXrayTrace =
            this.globalOptions.injectXrayTrace;
      }
      if ((options as BaseSaveOptions).injectLastModified === undefined) {
        (options as BaseSaveOptions).injectLastModified =
            this.globalOptions.injectLastModified;
      }
    }
    return options;
  }
}
