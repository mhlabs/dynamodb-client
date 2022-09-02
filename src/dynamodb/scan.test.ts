import {
  DynamoDBDocument,
  ScanCommand,
  ScanCommandInput
} from '@aws-sdk/lib-dynamodb';
import { mockClient } from 'aws-sdk-client-mock';

const dynamoDbDocumentMock = mockClient(DynamoDBDocument);
import { scan as tested } from './scan';

interface DynamoItem {
  id: number;
}

beforeEach(() => {
  dynamoDbDocumentMock.reset();
});

describe('scan', () => {
  it('should return items', async () => {
    dynamoDbDocumentMock.on(ScanCommand).resolves({ Items: [{ id: 5 }] });

    const result = await tested<DynamoItem>(
      dynamoDbDocumentMock as any,
      'table'
    );

    expect(dynamoDbDocumentMock.commandCalls(ScanCommand)).toHaveLength(1);
    expect(result).toHaveLength(1);
    expect(result[0].id).toEqual(5);
  });

  it('should fetch remaining items', async () => {
    dynamoDbDocumentMock
      .on(ScanCommand, { ['ExclusiveStartKey' as string]: undefined })
      .resolves({ Items: [{ id: 5 }], ['LastEvaluatedKey' as string]: 'key' })
      .on(ScanCommand, { ['ExclusiveStartKey' as string]: 'key' })
      .resolves({ Items: [{ id: 6 }] });

    const result = await tested<DynamoItem>(
      dynamoDbDocumentMock as any,
      'table'
    );

    expect(dynamoDbDocumentMock.commandCalls(ScanCommand)).toHaveLength(2);
    expect(result).toHaveLength(2);
    expect(result[0].id).toEqual(5);
    expect(result[1].id).toEqual(6);
  });

  it('should use arguments and extra options', async () => {
    dynamoDbDocumentMock.on(ScanCommand).resolves({ Items: [{ id: 5 }] });

    const table = 'table';
    const options = {
      Limit: 100,
      ConsistentRead: true
    } as ScanCommandInput;

    await tested(dynamoDbDocumentMock as any, table, options);

    const appliedArguments =
      dynamoDbDocumentMock.commandCalls(ScanCommand)[0].args[0].input;

    expect(appliedArguments.TableName).toBe(table);
    expect(appliedArguments.Limit).toBe(100);
    expect(appliedArguments.ConsistentRead).toBe(options.ConsistentRead);
  });
});
