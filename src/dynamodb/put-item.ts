import {
  DynamoDBDocument,
  PutCommand,
  PutCommandInput
} from '@aws-sdk/lib-dynamodb';

const ensureValidParameters = (
  documentClient: DynamoDBDocument,
  tableName: string,
  item: Record<string, any>
) => {
  if (!documentClient) throw new Error('documentClient is required.');
  if (!tableName) throw new Error('tableName is required.');
  if (!item) throw new Error('item is required.');
  if (typeof item !== 'object') throw new Error('item should be an object.');
};

export const putItem = async (
  documentClient: DynamoDBDocument,
  tableName: string,
  item: Record<string, any>,
  options?: PutCommandInput
) => {
  ensureValidParameters(documentClient, tableName, item);

  const input: PutCommandInput = {
    TableName: tableName,
    Item: item,
    ...options
  };

  const command = new PutCommand(input);
  await documentClient.send(command);

  return true;
};
