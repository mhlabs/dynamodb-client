import {
  DynamoDBDocument,
  ScanCommand,
  ScanCommandInput
} from '@aws-sdk/lib-dynamodb';

const ensureValidParameters = (
  documentClient: DynamoDBDocument,
  tableName: string
) => {
  if (!documentClient) throw new Error('documentClient is required.');
  if (!tableName) throw new Error('Table name is required.');
};

export const scan = async <T>(
  documentClient: DynamoDBDocument,
  tableName: string,
  options?: ScanCommandInput
): Promise<T[]> => {
  ensureValidParameters(documentClient, tableName);

  const items: T[] = [];
  let exclusiveStartKey;

  do {
    const input: ScanCommandInput = {
      TableName: tableName,
      ExclusiveStartKey: exclusiveStartKey,
      ...options
    };

    const scanCommand = new ScanCommand(input);
    // eslint-disable-next-line no-await-in-loop
    const scanResponse = await documentClient.send(scanCommand);

    exclusiveStartKey = scanResponse.LastEvaluatedKey;
    items.push(...(scanResponse.Items as T[]));
  } while (exclusiveStartKey);

  return items;
};