import {
  DynamoDBDocument,
  GetCommand,
  GetCommandInput
} from '@aws-sdk/lib-dynamodb';

const ensureValidParameters = (
  documentClient: DynamoDBDocument,
  tableName: string,
  key: Record<string, any>
) => {
  if (!documentClient) throw new Error('documentClient is required.');
  if (!tableName) throw new Error('tableName is required.');
  if (!key) throw new Error('key is required.');
  if (typeof key !== 'object') throw new Error('key should be an object.');
};

export const getItem = async <T>(
  documentClient: DynamoDBDocument,
  tableName: string,
  key: Record<string, any>,
  options?: GetCommandInput
): Promise<T | null> => {
  ensureValidParameters(documentClient, tableName, key);

  const input: GetCommandInput = {
    TableName: tableName,
    Key: key,
    ...options
  };

  const command = new GetCommand(input);
  const response = await documentClient.send(command);

  return (response?.Item as T) || null;
};
