const { GetCommand } = require('@aws-sdk/lib-dynamodb');

async function getItem(documentClient, tableName, key, options = undefined) {
  if (!tableName) throw new Error('Table name is required.');
  if (!key) throw new Error('Key is required.');
  if (typeof key !== 'object') throw new Error('Key should be an object.');

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
