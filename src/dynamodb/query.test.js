const { mockClient } = require('aws-sdk-client-mock');
const { DynamoDBDocument, QueryCommand } = require('@aws-sdk/lib-dynamodb');

const dynamoDBDocumentMock = mockClient(DynamoDBDocument);

const tested = require('./query');

describe('query', () => {
  it('should validate documentClient', async () => {
    await expect(tested()).rejects.toThrow('documentClient is required.');
  });

  it('should validate table', async () => {
    await expect(tested(dynamoDBDocumentMock, '')).rejects.toThrow(
      'table name is required.'
    );
  });

  it('should validate indexName', async () => {
    await expect(
      tested(dynamoDBDocumentMock, 'table', {}, true)
    ).rejects.toThrow('indexName is required.');
  });

  it('should validate keyCondition', async () => {
    await expect(tested(dynamoDBDocumentMock, 'table')).rejects.toThrow(
      'keyCondition is required.'
    );
  });

  it('should validate that key condition is object', async () => {
    await expect(tested(dynamoDBDocumentMock, 'table', 'a')).rejects.toThrow(
      'keyCondition should be an object.'
    );
  });

  it('should return items', async () => {
    dynamoDBDocumentMock
      .on(QueryCommand)
      .resolves({ Items: [{ SomeValue: 1 }] });

    const result = await tested(dynamoDBDocumentMock, 'table', {});

    expect(result).toHaveLength(1);
    expect(result[0].SomeValue).toBe(1);
  });

  // it('should generate proper query expression with one attribute', async () => {
  //   const yesterday = DateTime.now()
  //     .setZone(defaultTimeZone)
  //     .minus({ days: 1 })
  //     .startOf('day')
  //     .toISO();

  //   const attribute = {
  //     GeneratedFromDeliveryDate: yesterday
  //   };

  //   const result = tested.generateKeyCondition(attribute);

  //   expect(result.keyConditionExpression).toEqual(
  //     '#dynamo_GeneratedFromDeliveryDate = :val1'
  //   );
  //   expect(result.expressionAttributeValues).toEqual({ ':val1': yesterday });
  //   expect(result.expressionAttributeNames).toEqual({
  //     '#dynamo_GeneratedFromDeliveryDate': 'GeneratedFromDeliveryDate'
  //   });
  // });

  // it('should generate proper query expression with several attributes', async () => {
  //   const attributes = {
  //     Id: 1,
  //     Name: 'J Doe'
  //   };

  //   const result = tested.generateKeyCondition(attributes);

  //   expect(result.keyConditionExpression).toEqual(
  //     '#dynamo_Id = :val1 and #dynamo_Name = :val2'
  //   );
  //   expect(result.expressionAttributeValues).toEqual({
  //     ':val1': attributes.Id,
  //     ':val2': attributes.Name
  //   });

  //   expect(result.expressionAttributeNames).toEqual({
  //     '#dynamo_Id': 'Id',
  //     '#dynamo_Name': 'Name'
  //   });
  // });
});
