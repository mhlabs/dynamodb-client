import { QueryCommand, QueryCommandInput } from '@aws-sdk/lib-dynamodb';
import { MhDynamoClient } from '..';
import { sanitizeOutputs } from '../sanitize';
import { BaseFetchOptions } from '../types';
import { ensureValid, ensureValidQuery } from '../validation';

export interface QueryOptions extends BaseFetchOptions {
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

const createCommand = (options: QueryOptions) => {
  const commandInput = generateKeyCondition(options.keyCondition);
  commandInput.TableName = options.tableName;

  if (options.indexName) commandInput.IndexName = options.indexName;

  return new QueryCommand(commandInput);
};

export async function query<T>(
  this: MhDynamoClient,
  options: Omit<QueryOptions, 'indexName'>
): Promise<T[]> {
  options = this.mergeWithGlobalOptions(options);
  ensureValid(options, options.keyCondition, 'keyCondition');

  const command = createCommand({ ...options, indexName: '' });
  const data = await this.documentClient.send(command);

  return sanitizeOutputs(data.Items as T[], options);
}

export async function queryByIndex<T>(
  this: MhDynamoClient,
  options: QueryOptions
): Promise<T[]> {
  options = this.mergeWithGlobalOptions(options);
  ensureValidQuery(options, options.keyCondition, options.indexName);

  const command = createCommand(options);
  const data = await this.documentClient.send(command);

  return sanitizeOutputs(data.Items as T[], options);
}
