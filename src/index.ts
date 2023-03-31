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

interface Options {
  /** @type captureAwsv3Client function type */
  awsClientCapture?: (dynamoDbInstance: DynamoDB) => DynamoDB;
}

export class MhDynamoDbClient {
  private readonly client: DynamoDBDocumentClient;
  private readonly options: Options | undefined;

  constructor(
    options?: Options,
    dynamoDbClientConfig: DynamoDBClientConfig = {}
  ) {
    const dynamoDb = options?.awsClientCapture
      ? options.awsClientCapture(new DynamoDB(dynamoDbClientConfig))
      : new DynamoDB(dynamoDbClientConfig);
    this.client = DynamoDBDocumentClient.from(dynamoDb);
    this.options = options;
  }

  async getItem<T>(args: GetCommandInput): Promise<T> {
    const response = await this.client.send(new GetCommand(args));
    return response.Item as T;
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
}
