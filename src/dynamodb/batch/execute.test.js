const { mockClient } = require('aws-sdk-client-mock');
const {
  BatchWriteCommand,
  DynamoDBDocument
} = require('@aws-sdk/lib-dynamodb');

const dynamoDbDocumentMock = mockClient(DynamoDBDocument);

const tested = require('./execute');

beforeEach(() => {
  dynamoDbDocumentMock.reset();
});

const table = 'testTable';
const item = { id: 1 };

const command = new BatchWriteCommand({
  RequestItems: {
    [table]: [{ PutRequest: { Item: item } }]
  }
});

const retryOptions = {
  minMs: 0,
  maxMs: 0
};

describe('execute', () => {
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
        command,
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
        command,
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
        tested(dynamoDbDocumentMock, command, 1, 0, retryOptions)
      ).rejects.toThrow(
        'returned UnprocessedItems after 3 attempts (2 retries)'
      );

      expect(dynamoDbDocumentMock.commandCalls(BatchWriteCommand)).toHaveLength(
        3
      );
    });
  });
});
