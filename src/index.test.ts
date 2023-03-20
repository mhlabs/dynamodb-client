import {
  BatchGetCommand,
  BatchGetCommandInput,
  BatchWriteCommand,
  BatchWriteCommandInput,
  DeleteCommand,
  DynamoDBDocumentClient,
  GetCommand,
  PutCommand,
} from '@aws-sdk/lib-dynamodb';
import { mockClient } from 'aws-sdk-client-mock';
import { describe, expect, it } from 'vitest';
import { MhDynamoDbClient } from '.';

describe('test client', () => {
  it('should call getItem and return the result', async () => {
    const mockResponse = {
      Item: { id: '123', name: 'Test Item' },
    };

    const mhDynamoDbClient = new MhDynamoDbClient();
    const mock = mockClient(DynamoDBDocumentClient);
    mock.on(GetCommand).resolves(mockResponse);

    const params = { TableName: 'TestTable', Key: { id: '123' } };
    const result = await mhDynamoDbClient.getItem(params);

    expect(result).toEqual(mockResponse.Item);
  });

  it('should call putItem and return the response', async () => {
    const mockResponse = {};

    const mhDynamoDbClient = new MhDynamoDbClient();
    const mock = mockClient(DynamoDBDocumentClient);
    mock.on(PutCommand).resolves(mockResponse);

    const params = {
      TableName: 'TestTable',
      Item: { id: '123', name: 'Test Item' },
    };
    const result = await mhDynamoDbClient.putItem(params);

    expect(result).toEqual(mockResponse);
  });

  it('should call deleteItem and return the response', async () => {
    const mockResponse = {};

    const mhDynamoDbClient = new MhDynamoDbClient();
    const mock = mockClient(DynamoDBDocumentClient);
    mock.on(DeleteCommand).resolves(mockResponse);

    const params = { TableName: 'TestTable', Key: { id: { id: '123' } } };
    const result = await mhDynamoDbClient.deleteItem(params);

    expect(result).toEqual(mockResponse);
  });

  it('should call batch write and return the response', async () => {
    const mockResponse = {};

    const mhDynamoDbClient = new MhDynamoDbClient();
    const mock = mockClient(DynamoDBDocumentClient);
    mock.on(BatchWriteCommand).resolves(mockResponse);

    const params: BatchWriteCommandInput = {
      RequestItems: {
        sampleTable: [
          {
            PutRequest: {
              Item: { id: '123', name: 'Test Item' },
            },
          },
        ],
      },
    };
    const result = await mhDynamoDbClient.batchWriteItem(params);

    expect(result).toEqual(mockResponse);
  });

  it('should call batch get and return the result', async () => {
    const mockResponse = {};

    const mhDynamoDbClient = new MhDynamoDbClient();
    const mock = mockClient(DynamoDBDocumentClient);
    mock.on(BatchGetCommand).resolves(mockResponse);

    const params: BatchGetCommandInput = {
      RequestItems: {
        sampleTable: {
          Keys: [
            {
              id: '123',
            },
          ],
        },
      },
    };
    const result = await mhDynamoDbClient.batchGetItem(params);

    expect(result).toEqual(mockResponse);
  });
});
