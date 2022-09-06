import { QueryCommand, QueryCommandInput } from '@aws-sdk/lib-dynamodb';
import { BaseInput, MhDynamoClient } from '../..';

export interface QueryInput extends BaseInput {
  keyCondition: Record<string, any>;
  indexName: string;
}

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

const createCommand = (input: QueryInput) => {
  const commandInput = generateKeyCondition(input.keyCondition);
  commandInput.TableName = input.tableName;

  if (input.indexName) commandInput.IndexName = input.indexName;

  return new QueryCommand(commandInput);
};

export async function query<T>(
  this: MhDynamoClient,
  input: Omit<QueryInput, 'indexName'>
): Promise<T[]> {
  this.ensureValid(input.tableName, input.keyCondition, 'keyCondition');

  const command = createCommand({ ...input, indexName: '' });
  const data = await this.documentClient.send(command);

  return data.Items as T[];
}

export async function queryByIndex<T>(
  this: MhDynamoClient,
  input: QueryInput
): Promise<T[]> {
  this.ensureValidQuery(input.tableName, input.keyCondition, input.indexName);

  const command = createCommand(input);
  const data = await this.documentClient.send(command);

  return data.Items as T[];
}
