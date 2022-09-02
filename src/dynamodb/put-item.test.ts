import { mockClient } from 'aws-sdk-client-mock';
import {
  PutCommand,
  DynamoDBDocument,
  PutCommandInput
} from '@aws-sdk/lib-dynamodb';

const dynamoDbDocumentMock = mockClient(DynamoDBDocument);

import { putItem as tested } from './put-item';

beforeEach(() => {
  dynamoDbDocumentMock.reset();
});

describe('put', () => {
  it('should call update with arguments and extra options', async () => {
    const table = 'table';
    const options = {
      ConditionExpression: 'the condition'
    } as PutCommandInput;
    const item = { Id: 'x' };

    const result = await tested(
      dynamoDbDocumentMock as any,
      table,
      item,
      options
    );

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
