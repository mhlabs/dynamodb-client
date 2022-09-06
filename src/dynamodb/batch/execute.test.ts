import {
  BatchGetCommand,
  BatchWriteCommand,
  DynamoDBDocument
} from '@aws-sdk/lib-dynamodb';
import { mockClient } from 'aws-sdk-client-mock';
import { MhDynamoClient } from '../../..';

const dynamoDbDocumentMock = mockClient(DynamoDBDocument);

let client: MhDynamoClient;

beforeEach(() => {
  dynamoDbDocumentMock.reset();
  client = MhDynamoClient.fromDocumentClient(
    dynamoDbDocumentMock as unknown as DynamoDBDocument
  );
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

      const res = await client.execute({
        tableName: table,
        batchCommand: writeCommand,
        batchNo: 1,
        retryCount: 0,
        retryOptions,
        previousItems: []
      });

      expect(dynamoDbDocumentMock.commandCalls(BatchWriteCommand)).toHaveLength(
        2
      );
      expect(res).toBeTruthy();
    });

    it('should not retry if no UnprocessedItems', async () => {
      dynamoDbDocumentMock.on(BatchWriteCommand).resolves({
        UnprocessedItems: {}
      });

      const res = await client.execute({
        tableName: table,
        batchCommand: writeCommand,
        batchNo: 1,
        retryCount: 0,
        retryOptions,
        previousItems: []
      });

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
        client.execute({
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

      const res = await client.execute({
        tableName: table,
        batchCommand: getCommand,
        batchNo: 1,
        retryCount: 0,
        retryOptions,
        previousItems: []
      });

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

      const res = await client.execute({
        tableName: table,
        batchCommand: getCommand,
        batchNo: 1,
        retryCount: 0,
        retryOptions,
        previousItems: []
      });

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
        client.execute({
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
