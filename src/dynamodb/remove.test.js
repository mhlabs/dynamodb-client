const { mockClient } = require('aws-sdk-client-mock');
const { DeleteCommand, DynamoDBDocument } = require('@aws-sdk/lib-dynamodb');

const dynamoDbDocumentMock = mockClient(DynamoDBDocument);

const tested = require('./remove');

beforeEach(() => {
  dynamoDbDocumentMock.reset();
});

describe('put', () => {
  it('should validate documentClient', async () => {
    await expect(tested()).rejects.toThrow('documentClient is required.');
  });

  it('should validate table', async () => {
    await expect(tested(dynamoDbDocumentMock)).rejects.toThrow(
      'tableName is required.'
    );
  });

  it('should validate key', async () => {
    await expect(tested(dynamoDbDocumentMock, 'table')).rejects.toThrow(
      'key is required.'
    );
  });

  it('should validate that key is an object', async () => {
    await expect(tested(dynamoDbDocumentMock, 'table', 'x')).rejects.toThrow(
      'key should be an object.'
    );
  });

  it('should call delete with arguments and extra options', async () => {
    const table = 'table';
    const options = {
      ConditionExpression: 'the condition'
    };
    const key = { Id: 'x' };

    const result = await tested(dynamoDbDocumentMock, table, key, options);

    const appliedArguments =
      dynamoDbDocumentMock.commandCalls(DeleteCommand)[0].args[0].input;

    expect(appliedArguments.TableName).toBe(table);
    expect(appliedArguments.ConditionExpression).toBe(
      options.ConditionExpression
    );
    expect(appliedArguments.Key).toBe(key);
    expect(result).toBe(true);
  });
});
