const { mockClient } = require('aws-sdk-client-mock');
const { DynamoDBDocument, QueryCommand } = require('@aws-sdk/lib-dynamodb');

const dynamoDbDocumentMock = mockClient(DynamoDBDocument);

const tested = require('./query');

const table = 'table';

beforeEach(() => {
  dynamoDbDocumentMock.reset();
  dynamoDbDocumentMock.on(QueryCommand).resolves({ Items: [{ SomeValue: 1 }] });
});

describe('query', () => {
  it('should validate documentClient', async () => {
    await expect(tested()).rejects.toThrow('documentClient is required.');
  });

  it('should validate table', async () => {
    await expect(tested(dynamoDbDocumentMock, '')).rejects.toThrow(
      'table name is required.'
    );
  });

  it('should validate indexName', async () => {
    await expect(tested(dynamoDbDocumentMock, table, {}, true)).rejects.toThrow(
      'indexName is required.'
    );
  });

  it('should validate keyCondition', async () => {
    await expect(tested(dynamoDbDocumentMock, table)).rejects.toThrow(
      'keyCondition is required.'
    );
  });

  it('should validate that key condition is object', async () => {
    await expect(tested(dynamoDbDocumentMock, table, 'a')).rejects.toThrow(
      'keyCondition should be an object.'
    );
  });

  it('should return items', async () => {
    const result = await tested(dynamoDbDocumentMock, table, {});

    expect(result).toHaveLength(1);
    expect(result[0].SomeValue).toBe(1);
  });

  it('should apply index name for index query', async () => {
    await tested(dynamoDbDocumentMock, table, {}, true, 'index');

    const appliedArguments =
      dynamoDbDocumentMock.commandCalls(QueryCommand)[0].args[0].input;

    expect(appliedArguments.TableName).toBe(table);
    expect(appliedArguments.IndexName).toBe('index');
  });

  it('should generate proper query expression with one attribute', async () => {
    const name = 'someone';
    const attribute = { name };

    await tested(dynamoDbDocumentMock, table, attribute);

    const appliedArguments =
      dynamoDbDocumentMock.commandCalls(QueryCommand)[0].args[0].input;

    expect(appliedArguments.KeyConditionExpression).toEqual(
      '#dynamo_name = :val1'
    );
    expect(appliedArguments.ExpressionAttributeValues).toEqual({
      ':val1': name
    });
    expect(appliedArguments.ExpressionAttributeNames).toEqual({
      '#dynamo_name': 'name'
    });
  });

  it('should generate proper query expression with several attributes', async () => {
    const attributes = {
      id: 1,
      name: 'J Doe'
    };

    await tested(dynamoDbDocumentMock, table, attributes);

    const appliedArguments =
      dynamoDbDocumentMock.commandCalls(QueryCommand)[0].args[0].input;

    expect(appliedArguments.KeyConditionExpression).toEqual(
      '#dynamo_id = :val1 and #dynamo_name = :val2'
    );

    expect(appliedArguments.ExpressionAttributeValues).toEqual({
      ':val1': attributes.id,
      ':val2': attributes.name
    });

    expect(appliedArguments.ExpressionAttributeNames).toEqual({
      '#dynamo_id': 'id',
      '#dynamo_name': 'name'
    });
  });
});
