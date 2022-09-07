import {
  DynamoDB,
  DynamoDBClient,
  DynamoDBClientConfig
} from '@aws-sdk/client-dynamodb';
import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb';

import { execute, retryUnprocessedItems } from './src/dynamodb/batch/execute';
import { batchGet } from './src/dynamodb/batch/get';
import { batchRemove } from './src/dynamodb/batch/remove';
import { batchWrite } from './src/dynamodb/batch/write';
import { getItem } from './src/dynamodb/get-item';
import { putItem } from './src/dynamodb/put-item';
import { query, queryByIndex } from './src/dynamodb/query';
import { remove } from './src/dynamodb/remove';
import { scan } from './src/dynamodb/scan';
import { addXrayTraceId } from './src/middlewares/add-xray-trace-id';
import { removeXrayTraceId } from './src/middlewares/remove-xray-trace-id';
import {
  BaseFetchOptions,
  BaseOptions,
  BaseSaveOptions,
  MhDynamoClientOptions
} from './types';
import {
  ensureValid,
  ensureValidBase,
  ensureValidBatch,
  ensureValidBatchWrite,
  ensureValidQuery
} from './validation';

export class MhDynamoClient {
  private globalOptions: MhDynamoClientOptions;
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
    this.globalOptions = {
      injectXrayTrace: true,
      extractXrayTrace: true,
      ...options
    };

    if (client as DynamoDBDocument) {
      this.documentClient = client as DynamoDBDocument;
      return this;
    }

    const translateOptions = { ...this.globalOptions.translateConfig };
    if (!translateOptions?.marshallOptions) {
      translateOptions.marshallOptions = {
        removeUndefinedValues: true
      };
    }

    this.globalOptions.translateConfig = translateOptions;
    this.documentClient = DynamoDBDocument.from(
      client as DynamoDBClient,
      translateOptions
    );
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

  // Internal methods
  public execute = execute; // Public because of test
  protected retryUnprocessedItems = retryUnprocessedItems;

  // Validation
  protected ensureValidBase = ensureValidBase;
  protected ensureValid = ensureValid;
  protected ensureValidQuery = ensureValidQuery;
  protected ensureValidBatch = ensureValidBatch;
  protected ensureValidBatchWrite = ensureValidBatchWrite;

  // Middlewares
  protected middlewareRemoveXrayTraceId = removeXrayTraceId;
  protected middlewareAddXrayTraceId = addXrayTraceId;

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
    }
    if (options as BaseSaveOptions) {
      if ((options as BaseSaveOptions).injectXrayTrace === undefined) {
        (options as BaseSaveOptions).injectXrayTrace =
          this.globalOptions.injectXrayTrace;
      }
    }
    return options;
  }

  protected sanitizeOutputs<T>(output: T[], options: BaseFetchOptions): T[] {
    return output.map((i) => {
      return this.sanitizeOutput(i, options);
    });
  }

  protected sanitizeOutput<T>(output: T, options: BaseFetchOptions): T {
    let sanitized = { ...output };

    sanitized = this.middlewareRemoveXrayTraceId(
      sanitized,
      options.extractXrayTrace
    );
    return sanitized;
  }

  protected enrichInputs<T>(input: T[], options: BaseSaveOptions): T[] {
    return input.map((i) => {
      return this.enrichInput(i, options);
    });
  }

  protected enrichInput<T>(input: T, options: BaseSaveOptions): T {
    let enriched = { ...input };

    enriched = this.middlewareAddXrayTraceId(enriched, options.injectXrayTrace);
    return enriched;
  }
}
