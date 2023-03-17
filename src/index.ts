import {
  DeleteItemCommand,
  DeleteItemCommandInput,
  DynamoDBClient,
  DynamoDBClientConfig,
  GetItemCommand,
  GetItemCommandInput,
  PutItemCommand,
  PutItemCommandInput,
} from '@aws-sdk/client-dynamodb';

export class MhDynamoDbClient {
  private readonly client: DynamoDBClient;

  constructor(config: DynamoDBClientConfig) {
    this.client = new DynamoDBClient(config);
  }

  async getItem<T>(args: GetItemCommandInput): Promise<T> {
    const response = await this.client.send(new GetItemCommand(args));
    return response.Item as T;
  }

  async putItem<T>(args: PutItemCommandInput): Promise<any> {
    const response = await this.client.send(new PutItemCommand(args));
    return response;
  }

  async deleteItem<T>(args: DeleteItemCommandInput): Promise<any> {
    const response = await this.client.send(new DeleteItemCommand(args));
    return response;
  }
}
