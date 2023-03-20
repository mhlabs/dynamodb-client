import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import {
  BatchGetCommand,
  BatchGetCommandInput,
  BatchWriteCommand,
  BatchWriteCommandInput,
  DeleteCommand,
  DeleteCommandInput,
  DynamoDBDocumentClient,
  GetCommand,
  GetCommandInput,
  PutCommand,
  PutCommandInput,
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

  async putItem<T>(args: PutCommandInput): Promise<any> {
    const response = await this.client.send(new PutCommand(args));
    return response;
  }

  async deleteItem<T>(args: DeleteCommandInput): Promise<any> {
    const response = await this.client.send(new DeleteCommand(args));
    return response;
  }

  async batchWriteItem<T>(args: BatchWriteCommandInput): Promise<any> {
    const response = await this.client.send(new BatchWriteCommand(args));
    return response;
  }

  async batchGetItem<T>(args: BatchGetCommandInput): Promise<any> {
    const response = await this.client.send(new BatchGetCommand(args));
    return response;
  }
}
