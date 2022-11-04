import {
  BatchGetCommand,
  BatchWriteCommand,
  DynamoDBDocument
} from '@aws-sdk/lib-dynamodb';
import { mockClient } from 'aws-sdk-client-mock';
import { execute } from './execute';

const dynamoDbDocumentMock = mockClient(DynamoDBDocument);

beforeEach(() => {
  dynamoDbDocumentMock.reset();
  jest.resetAllMocks();
});

const table = 'testTable';

const retryOptions = {
  minMs: 0,
  maxMs: 0
};

describe('execute write', () => {
  const item = { id: 1 };

  const writeCommand = new BatchWriteCommand({
    RequestItems: {
      [table]: [{ PutRequest: { Item: item } }]
    }
  });

  describe('retry UnprocessedItems', () => {
    it('should retry once if UnprocessedItems', async () => {
      dynamoDbDocumentMock
        .on(BatchWriteCommand, {
          RequestItems: { [table]: [{ PutRequest: { Item: item } }] }
        })
        .resolves({
          UnprocessedItems: {
            [table]: [
              {
                PutRequest: {
                  Item: { id: 2 }
                }
              }
            ]
          }
        })
        .on(BatchWriteCommand, {
          RequestItems: { [table]: [{ PutRequest: { Item: { id: 2 } } }] }
        })
        .resolves({});

      const res = await execute(
        dynamoDbDocumentMock as unknown as DynamoDBDocument,
        {
          tableName: table,
          batchCommand: writeCommand,
          batchNo: 1,
          retryCount: 0,
          retryOptions,
          previousItems: []
        }
      );

      expect(dynamoDbDocumentMock.commandCalls(BatchWriteCommand)).toHaveLength(
        2
      );
      expect(res).toBeTruthy();
    });

    it('should not retry if no UnprocessedItems', async () => {
      dynamoDbDocumentMock.on(BatchWriteCommand).resolves({
        UnprocessedItems: {}
      });

      const res = await execute(
        dynamoDbDocumentMock as unknown as DynamoDBDocument,
        {
          tableName: table,
          batchCommand: writeCommand,
          batchNo: 1,
          retryCount: 0,
          retryOptions,
          previousItems: []
        }
      );

      expect(dynamoDbDocumentMock.commandCalls(BatchWriteCommand)).toHaveLength(
        1
      );
      expect(res).toBeTruthy();
    });

    it('should throw error after max retries', async () => {
      dynamoDbDocumentMock.on(BatchWriteCommand).resolves({
        UnprocessedItems: {
          [table]: [
            {
              PutRequest: {
                Item: { id: 1 }
              }
            }
          ]
        }
      });

      await expect(
        execute(dynamoDbDocumentMock as unknown as DynamoDBDocument, {
          tableName: table,
          batchCommand: writeCommand,
          batchNo: 1,
          retryCount: 0,
          retryOptions,
          previousItems: []
        })
      ).rejects.toThrow(
        'Batch: 1 - returned UnprocessedItems after 3 attempts (2 retries)'
      );

      expect(dynamoDbDocumentMock.commandCalls(BatchWriteCommand)).toHaveLength(
        3
      );
    });
  });
});

describe('execute get', () => {
  const keys = [{ id: 1 }, { id: 2 }];

  const getCommand = new BatchGetCommand({
    RequestItems: {
      [table]: {
        Keys: keys
      }
    }
  });

  describe('retry UnprocessedKeys', () => {
    it('should retry once if UnprocessedKeys and combine responses', async () => {
      dynamoDbDocumentMock
        .on(BatchGetCommand, {
          RequestItems: { [table]: { Keys: keys } }
        })
        .resolves({
          UnprocessedKeys: {
            [table]: {
              Keys: [{ id: 2 }]
            }
          },
          Responses: {
            [table]: [{ id: 1 }]
          }
        })
        .on(BatchGetCommand, {
          RequestItems: { [table]: { Keys: [{ id: 2 }] } }
        })
        .resolves({
          UnprocessedKeys: {},
          Responses: {
            [table]: [{ id: 2 }]
          }
        });

      const res = await execute(
        dynamoDbDocumentMock as unknown as DynamoDBDocument,
        {
          tableName: table,
          batchCommand: getCommand,
          batchNo: 1,
          retryCount: 0,
          retryOptions,
          previousItems: []
        }
      );

      expect(dynamoDbDocumentMock.commandCalls(BatchGetCommand)).toHaveLength(
        2
      );
      expect(res).toEqual(keys);
    });

    it('should not retry if no UnprocessedKeys', async () => {
      dynamoDbDocumentMock.on(BatchGetCommand).resolves({
        ['UnprocessedItems' as string]: {},
        Responses: {
          [table]: keys
        }
      });

      const res = await execute(
        dynamoDbDocumentMock as unknown as DynamoDBDocument,
        {
          tableName: table,
          batchCommand: getCommand,
          batchNo: 1,
          retryCount: 0,
          retryOptions,
          previousItems: []
        }
      );

      expect(dynamoDbDocumentMock.commandCalls(BatchGetCommand)).toHaveLength(
        1
      );
      expect(res).toEqual(keys);
    });

    it('should throw error after max retries and handle missing response', async () => {
      dynamoDbDocumentMock.on(BatchGetCommand).resolves({
        UnprocessedKeys: {
          [table]: {
            Keys: keys
          }
        }
      });

      await expect(
        execute(dynamoDbDocumentMock as unknown as DynamoDBDocument, {
          tableName: table,
          batchCommand: getCommand,
          batchNo: 1,
          retryCount: 0,
          retryOptions,
          previousItems: []
        })
      ).rejects.toThrow(
        'returned UnprocessedKeys after 3 attempts (2 retries)'
      );

      expect(dynamoDbDocumentMock.commandCalls(BatchGetCommand)).toHaveLength(
        3
      );
    });
  });
});

describe('delay between calls', () => {
  it('should use option for delay', async () => {
    jest.spyOn(global, 'setTimeout');

    const getCommand = new BatchGetCommand({
      RequestItems: {
        [table]: {
          Keys: []
        }
      }
    });

    dynamoDbDocumentMock.on(BatchGetCommand).resolves({
      Responses: {
        [table]: [{ id: 1 }]
      }
    });

    await execute(dynamoDbDocumentMock as unknown as DynamoDBDocument, {
      tableName: table,
      batchCommand: getCommand,
      batchNo: 1,
      retryCount: 0,
      retryOptions,
      delayMsBetweenCalls: 2,
      previousItems: []
    });

    expect(setTimeout).toHaveBeenCalledTimes(1);
    expect(setTimeout).toHaveBeenCalledWith(expect.any(Function), 2);
  });
});
