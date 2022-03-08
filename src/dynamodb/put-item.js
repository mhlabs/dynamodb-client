const { PutCommand } = require('@aws-sdk/lib-dynamodb');

function ensureValidParameters(documentClient, tableName, item) {
  if (!documentClient) throw new Error('documentClient is required.');
  if (!tableName) throw new Error('tableName is required.');
  if (!item) throw new Error('item is required.');
  if (typeof item !== 'object') throw new Error('item should be an object.');
}

async function putItem(documentClient, tableName, item, options = undefined) {
  ensureValidParameters(documentClient, tableName, item);

  const input = {
    TableName: tableName,
    Item: item,
    ...options
  };

  const command = new PutCommand(input);
  await documentClient.send(command);

  return true;
}

module.exports = putItem;
