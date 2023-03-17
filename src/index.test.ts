import {
  DeleteItemCommand,
  DynamoDBClient,
  GetItemCommand,
  PutItemCommand,
} from '@aws-sdk/client-dynamodb';
import { mockClient } from 'aws-sdk-client-mock';
import { describe, expect, it } from 'vitest';
import { MhDynamoDbClient } from '.';

describe('test client', () => {
  it('should call getItem and return the result', async () => {
    const mockResponse = {
      Item: { id: { S: '123' }, name: { S: 'Test Item' } },
    };

    const mhDynamoDbClient = new MhDynamoDbClient({});
    const mock = mockClient(DynamoDBClient);
    mock.on(GetItemCommand).resolves(mockResponse);

    const params = { TableName: 'TestTable', Key: { id: { S: '123' } } };
    const result = await mhDynamoDbClient.getItem(params);

    expect(result).toEqual(mockResponse.Item);
  });

  it('should call putItem and return the response', async () => {
    const mockResponse = {};

    const mhDynamoDbClient = new MhDynamoDbClient({});
    const mock = mockClient(DynamoDBClient);
    mock.on(PutItemCommand).resolves(mockResponse);

    const params = { TableName: 'TestTable', Item: { id: { S: '123' } } };
    const result = await mhDynamoDbClient.putItem(params);

    expect(result).toEqual(mockResponse);
  });

  it('should call deleteItem and return the response', async () => {
    const mockResponse = {};

    const mhDynamoDbClient = new MhDynamoDbClient({});
    const mock = mockClient(DynamoDBClient);
    mock.on(DeleteItemCommand).resolves(mockResponse);

    const params = { TableName: 'TestTable', Key: { id: { S: '123' } } };
    const result = await mhDynamoDbClient.deleteItem(params);

    expect(result).toEqual(mockResponse);
  });
});
