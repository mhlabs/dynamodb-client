const { mockClient } = require('aws-sdk-client-mock');
const {
  BatchGetCommand,
  BatchWriteCommand,
  DynamoDBDocument
} = require('@aws-sdk/lib-dynamodb');

const dynamoDbDocumentMock = mockClient(DynamoDBDocument);

const tested = require('./execute');

beforeEach(() => {
  dynamoDbDocumentMock.reset();
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
          RequestItems: { [table]: [{ PutRequest: { Item: { id: 1 } } }] }
        })
        .resolves({
          UnprocessedItems: {
            tableNameNotMatchingMock: [
              {
                PutRequest: {
                  Item: { id: 1 }
                }
              }
            ]
          }
        });

      const res = await tested(
        dynamoDbDocumentMock,
        writeCommand,
        1,
        0,
        retryOptions
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

      const res = await tested(
        dynamoDbDocumentMock,
        writeCommand,
        1,
        0,
        retryOptions
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
        tested(dynamoDbDocumentMock, writeCommand, 1, 0, retryOptions)
      ).rejects.toThrow(
        'returned UnprocessedItems after 3 attempts (2 retries)'
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

      const res = await tested(
        dynamoDbDocumentMock,
        getCommand,
        1,
        0,
        retryOptions
      );

      expect(dynamoDbDocumentMock.commandCalls(BatchGetCommand)).toHaveLength(
        2
      );
      expect(res).toEqual(keys);
    });

    it('should not retry if no UnprocessedKeys', async () => {
      dynamoDbDocumentMock.on(BatchGetCommand).resolves({
        UnprocessedItems: {},
        Responses: {
          [table]: keys
        }
      });

      const res = await tested(
        dynamoDbDocumentMock,
        getCommand,
        1,
        0,
        retryOptions
      );

      expect(dynamoDbDocumentMock.commandCalls(BatchGetCommand)).toHaveLength(
        1
      );
      expect(res).toEqual(keys);
    });

    it('should throw error after max retries and handle missing response', async () => {
      dynamoDbDocumentMock.on(BatchGetCommand).resolves({
        UnprocessedItems: {
          [table]: [
            {
              Keys: keys
            }
          ]
        }
      });

      await expect(
        tested(dynamoDbDocumentMock, getCommand, 1, 0, retryOptions)
      ).rejects.toThrow(
        'returned UnprocessedKeys after 3 attempts (2 retries)'
      );

      expect(dynamoDbDocumentMock.commandCalls(BatchGetCommand)).toHaveLength(
        3
      );
    });
  });
});
