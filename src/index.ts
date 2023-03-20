import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
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

export class MhDynamoDbClient {
  private readonly client: DynamoDBDocumentClient;

  constructor(client?: DynamoDBClient) {
    client = client ?? new DynamoDBClient({});
    this.client = DynamoDBDocumentClient.from(client);
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
