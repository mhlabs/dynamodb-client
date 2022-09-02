import { mockClient } from 'aws-sdk-client-mock';
import {
  DeleteCommand,
  DeleteCommandInput,
  DynamoDBDocument
} from '@aws-sdk/lib-dynamodb';

const dynamoDbDocumentMock = mockClient(DynamoDBDocument);

import { remove as tested } from './remove';

beforeEach(() => {
  dynamoDbDocumentMock.reset();
});

describe('remove', () => {
  it('should call delete with arguments and extra options', async () => {
    const table = 'table';
    const options = {
      ConditionExpression: 'the condition'
    } as DeleteCommandInput;
    const key = { Id: 'x' };

    const result = await tested(
      dynamoDbDocumentMock as any,
      table,
      key,
      options
    );

    const appliedArguments =
      dynamoDbDocumentMock.commandCalls(DeleteCommand)[0].args[0].input;

    expect(appliedArguments.TableName).toBe(table);
    expect(appliedArguments.ConditionExpression).toBe(
      options.ConditionExpression
    );
    expect(appliedArguments.Key).toBe(key);
    expect(result).toBe(true);
  });
});
