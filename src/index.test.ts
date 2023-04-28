import {
  BatchGetCommand,
  BatchGetCommandInput,
  BatchGetCommandOutput,
  BatchWriteCommand,
  BatchWriteCommandInput,
  BatchWriteCommandOutput,
  DeleteCommand,
  DeleteCommandOutput,
  DynamoDBDocumentClient,
  GetCommand,
  GetCommandOutput,
  PutCommand,
  PutCommandOutput,
} from '@aws-sdk/lib-dynamodb';
import { mockClient } from 'aws-sdk-client-mock';
import { MhDynamoDbClient } from '.';

describe('test client', () => {
  describe('constructor', () => {
    it('should call awsClientCapture with a new instance of DynamoDb if options.awsClientCapture is provided', () => {
      const mockedFunction = vi.fn((x) => x);

      const instance = new MhDynamoDbClient(
        { awsClientCapture: mockedFunction },
        {}
      );

      expect(mockedFunction).toHaveBeenCalled();
    });
  });

  describe('options', () => {
    it('should use default options', () => {
      const mhDynamoDbClient = new MhDynamoDbClient();
      const options = mhDynamoDbClient.getOptions();

      expect(options?.fetchOptions?.removeXrayTraceId).toEqual(true);
    });

    it('should overwrite removeXrayTraceId', () => {
      const mhDynamoDbClient = new MhDynamoDbClient({
        fetchOptions: { removeXrayTraceId: false },
      });
      const options = mhDynamoDbClient.getOptions();

      expect(options?.fetchOptions?.removeXrayTraceId).toEqual(false);
    });

    it('should add awsClientCapture', () => {
      const mockedFunction = vi.fn((x) => x);

      const mhDynamoDbClient = new MhDynamoDbClient({
        awsClientCapture: mockedFunction,
      });
      const options = mhDynamoDbClient.getOptions();

      expect(options?.fetchOptions?.removeXrayTraceId).toEqual(true);
      expect(options?.awsClientCapture).toEqual(mockedFunction);
    });

    it('should add awsClientCapture and removeXrayTraceId', () => {
      const mockedFunction = vi.fn((x) => x);

      const mhDynamoDbClient = new MhDynamoDbClient({
        awsClientCapture: mockedFunction,
        fetchOptions: { removeXrayTraceId: false },
      });
      const options = mhDynamoDbClient.getOptions();

      expect(options?.fetchOptions?.removeXrayTraceId).toEqual(false);
      expect(options?.awsClientCapture).toEqual(mockedFunction);
    });
  });

  it('should call getItem and return the result', async () => {
    const mockResponse: GetCommandOutput = {
      Item: { id: '123', name: 'Test Item' },
      $metadata: {},
    };
    const mock = mockClient(DynamoDBDocumentClient);
    mock.on(GetCommand).resolves(mockResponse);

    const mhDynamoDbClient = new MhDynamoDbClient();
    const params = { TableName: 'TestTable', Key: { id: '123' } };
    const result = await mhDynamoDbClient.getItem(params);

    expect(result).toEqual(mockResponse.Item);
  });

  it('should call putItem and return the response', async () => {
    const mockResponse: PutCommandOutput = {
      Attributes: { id: '123', name: 'Test Item' },
      $metadata: {},
    };

    const mock = mockClient(DynamoDBDocumentClient);
    mock.on(PutCommand).resolves(mockResponse);

    const mhDynamoDbClient = new MhDynamoDbClient();
    const params = {
      TableName: 'TestTable',
      Item: { id: '123', name: 'Test Item' },
    };
    const result = await mhDynamoDbClient.putItem(params);

    expect(result).toEqual(mockResponse);
  });

  it('should call deleteItem and return the response', async () => {
    const mockResponse: DeleteCommandOutput = {
      Attributes: { id: '123' },
      $metadata: {},
    };

    const mock = mockClient(DynamoDBDocumentClient);
    mock.on(DeleteCommand).resolves(mockResponse);

    const mhDynamoDbClient = new MhDynamoDbClient();
    const params = { TableName: 'TestTable', Key: { id: { id: '123' } } };
    const result = await mhDynamoDbClient.deleteItem(params);

    expect(result).toEqual(mockResponse);
  });

  it('should call batch write and return the response', async () => {
    const mockResponse: BatchWriteCommandOutput = {
      UnprocessedItems: {},
      $metadata: {},
    };

    const mock = mockClient(DynamoDBDocumentClient);
    mock.on(BatchWriteCommand).resolves(mockResponse);

    const mhDynamoDbClient = new MhDynamoDbClient();
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
    const mockResponse: BatchGetCommandOutput = {
      Responses: {
        sampleTable: [
          {
            id: '123',
          },
        ],
      },
      UnprocessedKeys: {},
      $metadata: {},
    };

    const mock = mockClient(DynamoDBDocumentClient);
    mock.on(BatchGetCommand).resolves(mockResponse);

    const mhDynamoDbClient = new MhDynamoDbClient();
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
