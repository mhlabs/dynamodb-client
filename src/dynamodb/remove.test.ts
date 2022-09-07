import {
  DeleteCommand,
  DeleteCommandInput,
  DynamoDBDocument
} from '@aws-sdk/lib-dynamodb';
import { mockClient } from 'aws-sdk-client-mock';
import { MhDynamoClient } from '..';

const dynamoDbDocumentMock = mockClient(DynamoDBDocument);

let client: MhDynamoClient;

beforeEach(() => {
  dynamoDbDocumentMock.reset();
  client = MhDynamoClient.fromDocumentClient(
    dynamoDbDocumentMock as unknown as DynamoDBDocument
  );
});

describe('remove', () => {
  it('should call delete with arguments and extra options', async () => {
    const table = 'table';
    const options = {
      ConditionExpression: 'the condition'
    } as DeleteCommandInput;
    const key = { Id: 'x' };

    const result = await client.remove({
      key,
      tableName: table,
      commandOptions: options
    });

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
