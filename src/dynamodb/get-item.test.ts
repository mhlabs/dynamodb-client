import { DynamoDBDocument, GetCommand } from '@aws-sdk/lib-dynamodb';
import { mockClient } from 'aws-sdk-client-mock';

const dynamoDbDocumentMock = mockClient(DynamoDBDocument);

import { GetItemCommandInput } from '@aws-sdk/client-dynamodb';

import { MhDynamoClient } from '..';

let client: MhDynamoClient;

interface DynamoItem {
  Id: number;
}

jest.useFakeTimers().setSystemTime(new Date("2022-11-10"));

beforeEach(() => {
  dynamoDbDocumentMock.reset();
  client = MhDynamoClient.fromDocumentClient(
    dynamoDbDocumentMock as unknown as DynamoDBDocument
  );
});

describe('get-item', () => {
  it('should return item', async () => {
    dynamoDbDocumentMock
      .on(GetCommand)
      .resolves({ Item: { Id: 5, _xray_trace_id: 'trace', _last_modified_at: new Date() } });

    const result = await client.getItem<DynamoItem>({
      tableName: 'table',
      key: {}
    });

    expect(result).toEqual({
      Id: 5
    });
  });

  it('should not sanitize xray trace', async () => {
    dynamoDbDocumentMock
        .on(GetCommand)
        .resolves({ Item: { Id: 5, _xray_trace_id: 'trace' } });

    const result = await client.getItem<DynamoItem>({
      tableName: 'table',
      key: {},
      extractXrayTrace: false
    });

    expect(result).toEqual({
      Id: 5,
      _xray_trace_id: 'trace'
    });
  });

  it('should not sanitize last modified at', async () => {
    dynamoDbDocumentMock
        .on(GetCommand)
        .resolves({ Item: { Id: 5, _last_modified_at: new Date() } });

    const result = await client.getItem<DynamoItem>({
      tableName: 'table',
      key: {},
      extractLastModifiedAt: false
    });

    expect(result).toEqual({
      Id: 5,
      _last_modified_at: new Date()
    });
  });

  it('should handle null response', async () => {
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
