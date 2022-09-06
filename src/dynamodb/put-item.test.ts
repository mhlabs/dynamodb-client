import {
  DynamoDBDocument,
  PutCommand,
  PutCommandInput
} from '@aws-sdk/lib-dynamodb';
import { mockClient } from 'aws-sdk-client-mock';

const dynamoDbDocumentMock = mockClient(DynamoDBDocument);

import { MhDynamoClient } from '../..';

let client: MhDynamoClient;

beforeEach(() => {
  dynamoDbDocumentMock.reset();
  client = MhDynamoClient.fromDocumentClient(
    dynamoDbDocumentMock as unknown as DynamoDBDocument
  );
});

describe('put', () => {
  it('should call update with arguments and extra options', async () => {
    const table = 'table';
    const options = {
      ConditionExpression: 'the condition'
    } as PutCommandInput;
    const item = { Id: 'x' };

    const result = await client.putItem({
      tableName: table,
      item,
      options
    });

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
