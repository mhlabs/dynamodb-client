const { mockClient } = require('aws-sdk-client-mock');
const {
  BatchWriteCommand,
  DynamoDBDocument
} = require('@aws-sdk/lib-dynamodb');

const dynamoDbDocumentMock = mockClient(DynamoDBDocument);

const { batchWrite, MAX_ITEMS_PER_BATCH } = require('./batch-write');

beforeEach(() => {
  dynamoDbDocumentMock.reset();
});

describe('batchWrite', () => {
  it('should split items into chunks of 25 (batchWrite limit)', async () => {
    const items = Array(60).fill({ id: 1 });
    const res = await batchWrite(dynamoDbDocumentMock, 'testTable', items);

    expect(dynamoDbDocumentMock.commandCalls(BatchWriteCommand)).toHaveLength(
      3
    );

    const commandCalls = dynamoDbDocumentMock.commandCalls(BatchWriteCommand);

    expect(commandCalls[0].args[0].input.RequestItems.testTable).toHaveLength(
      MAX_ITEMS_PER_BATCH
    );
    expect(commandCalls[1].args[0].input.RequestItems.testTable).toHaveLength(
      MAX_ITEMS_PER_BATCH
    );
    expect(commandCalls[2].args[0].input.RequestItems.testTable).toHaveLength(
      10
    );
    expect(res).toBe(true);
  });

  describe('retry UnprocessedItems', () => {
    it('should retry once if UnprocessedItems', async () => {
      const items = [{ id: 1 }];
      const table = 'testTable';

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

      const res = await batchWrite(dynamoDbDocumentMock, table, items, 0, 0);

      expect(dynamoDbDocumentMock.commandCalls(BatchWriteCommand)).toHaveLength(
        2
      );
      expect(res).toBeTruthy();
    });

    it('should not retry if no UnprocessedItems', async () => {
      const items = [{ id: 1 }, { id: 2 }, { id: 3 }];

      dynamoDbDocumentMock.on(BatchWriteCommand).resolves({
        UnprocessedItems: {}
      });

      const res = await batchWrite(
        dynamoDbDocumentMock,
        'testTable',
        items,
        0,
        0
      );

      expect(dynamoDbDocumentMock.commandCalls(BatchWriteCommand)).toHaveLength(
        1
      );
      expect(res).toBeTruthy();
    });

    it('should throw error after max retries', async () => {
      const items = [{ id: 1 }, { id: 2 }, { id: 3 }];
      const table = 'testTable';

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
        batchWrite(dynamoDbDocumentMock, table, items, 0, 0)
      ).rejects.toThrow(
        'returned UnprocessedItems after 3 attempts (2 retries)'
      );

      expect(dynamoDbDocumentMock.commandCalls(BatchWriteCommand)).toHaveLength(
        3
      );
    });
  });
});
