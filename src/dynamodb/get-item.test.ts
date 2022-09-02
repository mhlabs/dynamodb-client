import { mockClient } from 'aws-sdk-client-mock';
import { GetCommand, DynamoDBDocument } from '@aws-sdk/lib-dynamodb';

const dynamoDbDocumentMock = mockClient(DynamoDBDocument);

import { getItem as tested } from './get-item';
import { GetItemCommandInput } from '@aws-sdk/client-dynamodb';

beforeEach(() => {
  dynamoDbDocumentMock.reset();
});

describe('get-item', () => {
  it('should return item', async () => {
    dynamoDbDocumentMock.on(GetCommand).resolves({ Item: { Id: 5 } });

    const result = await tested<{ Id: number }>(
      dynamoDbDocumentMock as any,
      'table',
      {}
    );
    expect(result?.Id).toEqual(5);
  });

  it('should handle null repsonse', async () => {
    dynamoDbDocumentMock.on(GetCommand).resolves(null as any);

    const result = await tested(dynamoDbDocumentMock as any, 'table', {});
    expect(result).toBeNull();
  });

  it('should use arguments and extra options', async () => {
    const table = 'table';
    const key = { Id: 5 };
    const options = {
      AttributesToGet: ['x', 'z'],
      ConsistentRead: true
    } as GetItemCommandInput;

    await tested(dynamoDbDocumentMock as any, table, key, options);

    const appliedArguments =
      dynamoDbDocumentMock.commandCalls(GetCommand)[0].args[0].input;

    expect(appliedArguments.TableName).toBe(table);
    expect(appliedArguments.Key).toBe(key);
    expect(appliedArguments.AttributesToGet).toBe(options.AttributesToGet);
    expect(appliedArguments.ConsistentRead).toBe(options.ConsistentRead);
  });
});
