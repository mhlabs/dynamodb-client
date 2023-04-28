import { DynamoDB, DynamoDBClientConfig } from '@aws-sdk/client-dynamodb';
import {
  BatchGetCommand,
  BatchGetCommandInput,
  BatchGetCommandOutput,
  BatchWriteCommand,
  BatchWriteCommandInput,
  BatchWriteCommandOutput,
  DeleteCommand,
  DeleteCommandInput,
  DeleteCommandOutput,
  DynamoDBDocumentClient,
  GetCommand,
  GetCommandInput,
  PutCommand,
  PutCommandInput,
  PutCommandOutput,
} from '@aws-sdk/lib-dynamodb';
import { WithXrayTraceId } from './types';

interface Options {
  /** @type captureAwsv3Client function type */
  awsClientCapture?: (dynamoDbInstance: DynamoDB) => DynamoDB;
  fetchOptions?: {
    removeXrayTraceId: boolean;
  };
}

const defaultOptions: Options = {
  fetchOptions: {
    removeXrayTraceId: true,
  },
};

export class MhDynamoDbClient {
  private readonly client: DynamoDBDocumentClient;
  private readonly options: Options | undefined;

  constructor(
    options?: Options,
    dynamoDbClientConfig: DynamoDBClientConfig = {}
  ) {
    this.options = this.mergeOptions(defaultOptions, options);

    const dynamoDb = options?.awsClientCapture
      ? options.awsClientCapture(new DynamoDB(dynamoDbClientConfig))
      : new DynamoDB(dynamoDbClientConfig);

    this.client = DynamoDBDocumentClient.from(dynamoDb);
  }

  async getItem<T>(args: GetCommandInput): Promise<T> {
    const response = await this.client.send(new GetCommand(args));
    return this.sanitizeItem(response.Item as T);
  }

  async putItem(args: PutCommandInput): Promise<PutCommandOutput> {
    const response = await this.client.send(new PutCommand(args));
    return response;
  }

  async deleteItem(args: DeleteCommandInput): Promise<DeleteCommandOutput> {
    const response = await this.client.send(new DeleteCommand(args));
    return response;
  }

  async batchWriteItem(
    args: BatchWriteCommandInput
  ): Promise<BatchWriteCommandOutput> {
    const response = await this.client.send(new BatchWriteCommand(args));
    return response;
  }

  async batchGetItem(
    args: BatchGetCommandInput
  ): Promise<BatchGetCommandOutput> {
    const response = await this.client.send(new BatchGetCommand(args));
    return response;
  }

  getOptions(): Options | undefined {
    return this.options;
  }

  private mergeOptions(opt1, opt2): Options | undefined {
    if (!opt1) return opt2;
    if (!opt2) return opt1;

    return {
      ...opt1,
      ...opt2,
      fetchOptions: {
        ...opt1.fetchOptions,
        ...opt2.fetchOptions,
      },
    };
  }

  private sanitizeItem<T>(item: T): T {
    if (!item) return item;

    if (this.options?.fetchOptions?.removeXrayTraceId) {
      item = this.removeXrayTraceId(item);
    }

    return item;
  }

  private removeXrayTraceId<T>(item): T {
    if (!('_xray_trace_id' in item)) return item;

    const { _xray_trace_id, ...restOfItem } = item as WithXrayTraceId<T>;
    return restOfItem as T;
  }
}
