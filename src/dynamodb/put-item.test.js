const { mockClient } = require('aws-sdk-client-mock');
const { PutCommand, DynamoDBDocument } = require('@aws-sdk/lib-dynamodb');

const dynamoDbDocumentMock = mockClient(DynamoDBDocument);

const tested = require('./put-item');

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

  it('should validate item', async () => {
    await expect(tested(dynamoDbDocumentMock, 'table')).rejects.toThrow(
      'item is required.'
    );
  });

  it('should validate that item is an object', async () => {
    await expect(tested(dynamoDbDocumentMock, 'table', 'x')).rejects.toThrow(
      'item should be an object.'
    );
  });

  it('should call update with arguments and extra options', async () => {
    const table = 'table';
    const options = {
      ConditionExpression: 'the condition'
    };
    const item = { Id: 'x' };

    const result = await tested(dynamoDbDocumentMock, table, item, options);

    const appliedArguments =
      dynamoDbDocumentMock.commandCalls(PutCommand)[0].args[0].input;

    expect(appliedArguments.TableName).toBe(table);
    expect(appliedArguments.ConditionExpression).toBe(
      options.ConditionExpression
    );
    expect(appliedArguments.Item).toBe(item);
    expect(result).toBe(true);
  });
});
