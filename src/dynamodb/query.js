const { QueryCommand } = require('@aws-sdk/lib-dynamodb');

function generateKeyCondition(attributeUpdates) {
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
  };
}

function ensureValidParameters(
  documentClient,
  tableName,
  keyCondition,
  indexQuery,
  indexName
) {
  if (!documentClient) throw new Error('documentClient is required.');
  if (!tableName) throw new Error('table name is required.');
  if (indexQuery && !indexName) throw new Error('indexName is required.');
  if (!keyCondition) throw new Error('keyCondition is required.');
  if (typeof keyCondition !== 'object')
    throw new Error('keyCondition should be an object.');
}

async function query(
  documentClient,
  tableName,
  keyCondition,
  indexQuery = false,
  indexName = undefined
) {
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

  return data.Items;
}

module.exports = query;
