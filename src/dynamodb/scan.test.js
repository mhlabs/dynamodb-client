const { mockClient } = require('aws-sdk-client-mock');
const { ScanCommand, DynamoDBDocument } = require('@aws-sdk/lib-dynamodb');

const dynamoDbDocumentMock = mockClient(DynamoDBDocument);

const tested = require('./scan');

beforeEach(() => {
  dynamoDbDocumentMock.reset();
});

describe('scan', () => {
  it('should validate documentClient', async () => {
    await expect(tested()).rejects.toThrow('documentClient is required.');
  });

  it('should validate table', async () => {
    await expect(tested(dynamoDbDocumentMock)).rejects.toThrow(
      'Table name is required.'
    );
  });

  it('should return items', async () => {
    dynamoDbDocumentMock.on(ScanCommand).resolves({ Items: [{ id: 5 }] });

    const result = await tested(dynamoDbDocumentMock, 'table');

    expect(dynamoDbDocumentMock.commandCalls(ScanCommand)).toHaveLength(1);
    expect(result).toHaveLength(1);
    expect(result[0].id).toEqual(5);
  });

  it('should fetch remaining items', async () => {
    dynamoDbDocumentMock
      .on(ScanCommand, { ExclusiveStartKey: undefined })
      .resolves({ Items: [{ id: 5 }], LastEvaluatedKey: 'key' })
      .on(ScanCommand, { ExclusiveStartKey: 'key' })
      .resolves({ Items: [{ id: 6 }] });

    const result = await tested(dynamoDbDocumentMock, 'table');

    expect(dynamoDbDocumentMock.commandCalls(ScanCommand)).toHaveLength(2);
    expect(result).toHaveLength(2);
    expect(result[0].id).toEqual(5);
    expect(result[1].id).toEqual(6);
  });

  it('should use arguments and extra options', async () => {
    dynamoDbDocumentMock.on(ScanCommand).resolves({ Items: [{ id: 5 }] });

    const table = 'table';
    const options = {
      Limit: 100,
      ConsistentRead: true
    };

    await tested(dynamoDbDocumentMock, table, options);

    const appliedArguments =
      dynamoDbDocumentMock.commandCalls(ScanCommand)[0].args[0].input;

    expect(appliedArguments.TableName).toBe(table);
    expect(appliedArguments.Limit).toBe(100);
    expect(appliedArguments.ConsistentRead).toBe(options.ConsistentRead);
  });
});
