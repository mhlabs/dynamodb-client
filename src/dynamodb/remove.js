const { DeleteCommand } = require('@aws-sdk/lib-dynamodb');

function ensureValidParameters(documentClient, tableName, key) {
  if (!documentClient) throw new Error('documentClient is required.');
  if (!tableName) throw new Error('tableName is required.');
  if (!key) throw new Error('key is required.');
  if (typeof key !== 'object') throw new Error('key should be an object.');
}

async function remove(documentClient, tableName, key, options = undefined) {
  ensureValidParameters(documentClient, tableName, key);

  const deleteCommand = new DeleteCommand({
    TableName: tableName,
    Key: key,
    ...options
  });

  await documentClient.send(deleteCommand);

  return true;
}

module.exports = remove;
