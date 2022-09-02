import {
  DeleteCommand,
  DeleteCommandInput,
  DynamoDBDocument
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

export const remove = async (
  documentClient: DynamoDBDocument,
  tableName: string,
  key: Record<string, any>,
  options?: DeleteCommandInput
) => {
  ensureValidParameters(documentClient, tableName, key);

  const input: DeleteCommandInput = {
    TableName: tableName,
    Key: key,
    ...options
  };

  const deleteCommand = new DeleteCommand(input);
  await documentClient.send(deleteCommand);

  return true;
};
