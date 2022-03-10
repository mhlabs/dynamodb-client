const { mockClient } = require('aws-sdk-client-mock');
const { BatchGetCommand, DynamoDBDocument } = require('@aws-sdk/lib-dynamodb');
const constants = require('./constants');

const dynamoDbDocumentMock = mockClient(DynamoDBDocument);

const tested = require('./get');

beforeEach(() => {
  dynamoDbDocumentMock.reset();
});

const table = 'testTable';

describe('batch get', () => {
  it('should validate document client', async () => {
    await expect(tested(null, '', {})).rejects.toThrow(
      'documentClient is required.'
    );
  });

  it('should validate table', async () => {
    await expect(tested({}, '', {})).rejects.toThrow('tableName is required.');
  });

  it('should validate keys', async () => {
    await expect(tested({}, 'table')).rejects.toThrow('Key list is required.');
  });

  it('should validate that keys are objects', async () => {
    const keys = [{}, 'b'];
    await expect(tested({}, 'table', keys)).rejects.toThrow(
      'All keys should be objects.'
    );
  });

  it('should split keys into chunks of max key size limit and merge responses', async () => {
    const keys = Array(constants.MAX_KEYS_PER_BATCH_GET + 1).fill({ id: 1 });

    dynamoDbDocumentMock.on(BatchGetCommand).resolves({
      Responses: {
        [table]: [{ id: 1 }]
      }
    });

    const result = await tested(dynamoDbDocumentMock, 'testTable', keys);

    expect(result).toHaveLength(2);
    expect(dynamoDbDocumentMock.commandCalls(BatchGetCommand)).toHaveLength(2);

    const firstCallArguments =
      dynamoDbDocumentMock.commandCalls(BatchGetCommand)[0].args[0].input;

    expect(firstCallArguments.RequestItems[table].Keys).toHaveLength(
      constants.MAX_KEYS_PER_BATCH_GET
    );

    const secondCallArguments =
      dynamoDbDocumentMock.commandCalls(BatchGetCommand)[1].args[0].input;

    expect(secondCallArguments.RequestItems[table].Keys).toHaveLength(1);
  });

  it('should retry once if UnprocessedItems', async () => {
    const keys = [{ id: 1 }, { id: 2 }];

    dynamoDbDocumentMock
      .on(BatchGetCommand, {
        RequestItems: {
          [table]: { Keys: [{ id: 2 }] }
        }
      })
      .resolves({
        Responses: {
          [table]: keys
        }
      })
      .on(BatchGetCommand, {
        RequestItems: {
          [table]: { Keys: [{ id: 1 }, { id: 2 }] }
        }
      })
      .resolves({
        Responses: {
          [table]: keys
        },
        UnprocessedKeys: {
          [table]: {
            Keys: [{ id: 2 }]
          }
        }
      });

    await tested(dynamoDbDocumentMock, table, keys);

    expect(dynamoDbDocumentMock.commandCalls(BatchGetCommand)).toHaveLength(2);
  });

  it('should not retry if no UnprocessedItems', async () => {
    const keys = [{ id: 1 }, { id: 2 }];

    dynamoDbDocumentMock.on(BatchGetCommand).resolves({
      Responses: {
        [table]: keys
      }
    });

    await tested(dynamoDbDocumentMock, table, keys);

    expect(dynamoDbDocumentMock.commandCalls(BatchGetCommand)).toHaveLength(1);
  });

  it('should throw error after max retries', async () => {
    const keys = [{ id: 1 }, { id: 2 }];

    dynamoDbDocumentMock.on(BatchGetCommand).resolves({
      Responses: {
        [table]: keys
      },
      UnprocessedKeys: {
        [table]: {
          Keys: [{ id: 2 }]
        }
      }
    });

    await expect(tested(dynamoDbDocumentMock, table, keys)).rejects.toThrow(
      'batchGet error: returned UnprocessedKeys after retry'
    );

    expect(dynamoDbDocumentMock.commandCalls(BatchGetCommand)).toHaveLength(2);
  });

  it('should call batch get with extra options', async () => {
    const options = {
      ConsistentRead: true
    };

    const keys = [{ Id: 'x' }];

    dynamoDbDocumentMock.on(BatchGetCommand).resolves({
      Responses: {
        [table]: keys
      }
    });

    await tested(dynamoDbDocumentMock, table, keys, options);

    const firstCallArguments =
      dynamoDbDocumentMock.commandCalls(BatchGetCommand)[0].args[0].input;

    expect(firstCallArguments.RequestItems[table].ConsistentRead).toBe(true);
  });
});
