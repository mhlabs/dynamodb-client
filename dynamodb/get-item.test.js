const { mockClient } = require('aws-sdk-client-mock');
const { GetCommand, DynamoDBDocument } = require('@aws-sdk/lib-dynamodb');

const dynamoDbDocumentMock = mockClient(DynamoDBDocument);

const getItem = require('./get-item');

describe('get-item', () => {
  it('should validate table', async () => {
    await expect(getItem(dynamoDbDocumentMock, '', {})).rejects.toThrow(
      'Table name is required.'
    );
  });

  it('should validate key', async () => {
    await expect(getItem(dynamoDbDocumentMock, 'table')).rejects.toThrow(
      'Key is required.'
    );
  });

  it('should validate that key key is object', async () => {
    await expect(getItem(dynamoDbDocumentMock, 'table', 'id')).rejects.toThrow(
      'Key should be an object.'
    );
  });

  it('should return item', async () => {
    dynamoDbDocumentMock.on(GetCommand).resolves({ Item: { Id: 5 } });

    const result = await getItem(dynamoDbDocumentMock, 'table', {});
    expect(result.Id).toEqual(5);
  });

  it('should handle null repsonse', async () => {
    dynamoDbDocumentMock.on(GetCommand).resolves(null);

    const result = await getItem(dynamoDbDocumentMock, 'table', {});
    expect(result).toBeNull();
  });
});
