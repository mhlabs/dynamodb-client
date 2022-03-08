const { GetCommand } = require('@aws-sdk/lib-dynamodb');

function ensureValidParameters(documentClient, tableName, key) {
  if (!documentClient) throw new Error('documentClient is required.');
  if (!tableName) throw new Error('tableName is required.');
  if (!key) throw new Error('key is required.');
  if (typeof key !== 'object') throw new Error('key should be an object.');
}

async function getItem(documentClient, tableName, key, options = undefined) {
  ensureValidParameters(documentClient, tableName, key);

  const input = {
    TableName: tableName,
    Key: key,
    ...options
  };

  const command = new GetCommand(input);
  const response = await documentClient.send(command);

  return response?.Item || null;
}

module.exports = getItem;
