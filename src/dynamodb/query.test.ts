import { mockClient } from 'aws-sdk-client-mock';
import { DynamoDBDocument, QueryCommand } from '@aws-sdk/lib-dynamodb';

const dynamoDbDocumentMock = mockClient(DynamoDBDocument);

import { query as tested } from './query';

const table = 'table';

interface DynamoItem {
  SomeValue: number;
}

beforeEach(() => {
  dynamoDbDocumentMock.reset();
  dynamoDbDocumentMock.on(QueryCommand).resolves({ Items: [{ SomeValue: 1 }] });
});

describe('query', () => {
  it('should return items', async () => {
    const result = await tested<DynamoItem>(
      dynamoDbDocumentMock as any,
      table,
      {}
    );

    expect(result).toHaveLength(1);
    expect(result[0].SomeValue).toBe(1);
  });

  it('should apply index name for index query', async () => {
    await tested(dynamoDbDocumentMock as any, table, {}, true, 'index');

    const appliedArguments =
      dynamoDbDocumentMock.commandCalls(QueryCommand)[0].args[0].input;

    expect(appliedArguments.TableName).toBe(table);
    expect(appliedArguments.IndexName).toBe('index');
  });

  it('should generate proper query expression with one attribute', async () => {
    const name = 'someone';
    const attribute = { name };

    await tested(dynamoDbDocumentMock as any, table, attribute);

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

    await tested(dynamoDbDocumentMock as any, table, attributes);

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
