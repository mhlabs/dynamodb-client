import { DynamoDBDocument, GetCommand } from '@aws-sdk/lib-dynamodb';
import { mockClient } from 'aws-sdk-client-mock';

const dynamoDbDocumentMock = mockClient(DynamoDBDocument);

import { GetItemCommandInput } from '@aws-sdk/client-dynamodb';

import { MhDynamoClient } from '../..';

let client: MhDynamoClient;

interface DynamoItem {
  Id: number;
}

beforeEach(() => {
  dynamoDbDocumentMock.reset();
  client = MhDynamoClient.fromDocumentClient(
    dynamoDbDocumentMock as unknown as DynamoDBDocument
  );
});

describe('get-item', () => {
  it('should return item', async () => {
    dynamoDbDocumentMock.on(GetCommand).resolves({ Item: { Id: 5 } });

    const result = await client.getItem<DynamoItem>({
      tableName: 'table',
      key: {}
    });

    expect(result?.Id).toEqual(5);
  });

  it('should handle null repsonse', async () => {
    dynamoDbDocumentMock.on(GetCommand).resolves(null as any);

    const result = await client.getItem<DynamoItem>({
      tableName: 'table',
      key: {}
    });
    expect(result).toBeNull();
  });

  it('should use arguments and extra options', async () => {
    const table = 'table';
    const key = { Id: 5 };
    const options = {
      AttributesToGet: ['x', 'z'],
      ConsistentRead: true
    } as GetItemCommandInput;

    await client.getItem<DynamoItem>({
      tableName: table,
      key,
      commandOptions: options
    });

    const appliedArguments =
      dynamoDbDocumentMock.commandCalls(GetCommand)[0].args[0].input;

    expect(appliedArguments.TableName).toBe(table);
    expect(appliedArguments.Key).toBe(key);
    expect(appliedArguments.AttributesToGet).toBe(options.AttributesToGet);
    expect(appliedArguments.ConsistentRead).toBe(options.ConsistentRead);
  });
});
