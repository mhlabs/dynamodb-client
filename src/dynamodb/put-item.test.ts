import {
  DynamoDBDocument,
  PutCommand,
  PutCommandInput
} from '@aws-sdk/lib-dynamodb';
import { mockClient } from 'aws-sdk-client-mock';

const dynamoDbDocumentMock = mockClient(DynamoDBDocument);

import { MhDynamoClient } from '..';

let client: MhDynamoClient;
const env = process.env;

jest.useFakeTimers().setSystemTime(new Date("2022-11-10"));

beforeEach(() => {
  process.env = { ...env };
  dynamoDbDocumentMock.reset();
  client = MhDynamoClient.fromDocumentClient(
    dynamoDbDocumentMock as unknown as DynamoDBDocument
  );
});

afterEach(() => {
  process.env = env;
});

describe('put', () => {
  it('should call update with arguments and extra options', async () => {
    const table = 'table';
    const options = {
      ConditionExpression: 'the condition'
    } as PutCommandInput;
    const item = { Id: 'x', _last_modified: (new Date()).toISOString() };

    const result = await client.putItem({
      tableName: table,
      item,
      commandOptions: options
    });

    const appliedArguments =
      dynamoDbDocumentMock.commandCalls(PutCommand)[0].args[0].input;

    expect(appliedArguments.TableName).toBe(table);
    expect(appliedArguments.ConditionExpression).toBe(
      options.ConditionExpression
    );
    expect(appliedArguments.Item).toEqual(item);
    expect(result).toBe(true);
  });

  it('should always enrich with _last_modified', async () => {
    const table = 'table';
    const options = {
      ConditionExpression: 'the condition'
    } as PutCommandInput;
    const item = { Id: 'x' };

    const result = await client.putItem({
      tableName: table,
      item,
      commandOptions: options
    });

    const appliedArguments =
        dynamoDbDocumentMock.commandCalls(PutCommand)[0].args[0].input;
    expect(appliedArguments.Item).toEqual({
      ...item,
      _last_modified: (new Date()).toISOString(),
    });
    expect(result).toBe(true);
  });

  it('should enrich item', async () => {
    process.env._X_AMZN_TRACE_ID = 'trace';
    const table = 'table';
    const options = {
      ConditionExpression: 'the condition'
    } as PutCommandInput;
    const item = { Id: 'x' };

    const result = await client.putItem({
      tableName: table,
      item,
      commandOptions: options
    });

    const appliedArguments =
      dynamoDbDocumentMock.commandCalls(PutCommand)[0].args[0].input;
    expect(appliedArguments.Item).toEqual({
      ...item,
      _xray_trace_id: 'trace',
      _last_modified: (new Date()).toISOString(),
    });
    expect(result).toBe(true);
  });

  it('should not enrich item', async () => {
    process.env._X_AMZN_TRACE_ID = 'trace';
    const table = 'table';
    const options = {
      ConditionExpression: 'the condition'
    } as PutCommandInput;
    const item = { Id: 'x' };

    const result = await client.putItem({
      tableName: table,
      item,
      commandOptions: options,
      injectXrayTrace: false,
      injectLastModified: false,
    });

    const appliedArguments =
      dynamoDbDocumentMock.commandCalls(PutCommand)[0].args[0].input;
    expect(appliedArguments.Item).toEqual(item);
    expect(result).toBe(true);
  });
});
