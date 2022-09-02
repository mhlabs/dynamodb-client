import {
  DynamoDBDocument,
  QueryCommand,
  QueryCommandInput
} from '@aws-sdk/lib-dynamodb';

const generateKeyCondition = (
  attributeUpdates: Record<string, any>
): QueryCommandInput => {
  let keyConditionExpression = '';
  const expressionAttributeValues = {};
  const expressionAttributeNames = {};

  let attributeIndex = 0;

  Object.keys(attributeUpdates).forEach((key) => {
    attributeIndex += 1;
    if (keyConditionExpression) {
      keyConditionExpression += ' and ';
    }

    const attributeProperty = `:val${attributeIndex}`;

    const expressionAttributeName = `#dynamo_${key}`;
    expressionAttributeNames[expressionAttributeName] = key;

    keyConditionExpression += `${expressionAttributeName} = ${attributeProperty}`;
    expressionAttributeValues[attributeProperty] = attributeUpdates[key];
  });

  return {
    ExpressionAttributeValues: expressionAttributeValues,
    KeyConditionExpression: keyConditionExpression,
    ExpressionAttributeNames: expressionAttributeNames
  } as QueryCommandInput;
};

const ensureValidParameters = (
  documentClient: DynamoDBDocument,
  tableName: string,
  keyCondition: Record<string, any>,
  indexQuery: boolean,
  indexName?: string
) => {
  if (!documentClient) throw new Error('documentClient is required.');
  if (!tableName) throw new Error('table name is required.');
  if (indexQuery && !indexName) throw new Error('indexName is required.');
  if (!keyCondition) throw new Error('keyCondition is required.');
  if (typeof keyCondition !== 'object')
    throw new Error('keyCondition should be an object.');
};

export const query = async <T>(
  documentClient: DynamoDBDocument,
  tableName: string,
  keyCondition: Record<string, any>,
  indexQuery = false,
  indexName?: string
): Promise<T[]> => {
  ensureValidParameters(
    documentClient,
    tableName,
    keyCondition,
    indexQuery,
    indexName
  );

  const input = generateKeyCondition(keyCondition);

  input.TableName = tableName;
  if (indexName) input.IndexName = indexName;

  const command = new QueryCommand(input);
  const data = await documentClient.send(command);

  return data.Items as T[];
};
