const { ScanCommand } = require('@aws-sdk/lib-dynamodb');

function ensureValidParameters(documentClient, tableName) {
  if (!documentClient) throw new Error('documentClient is required.');
  if (!tableName) throw new Error('Table name is required.');
}

async function scan(documentClient, tableName, options = undefined) {
  ensureValidParameters(documentClient, tableName);

  const items = [];
  let exclusiveStartKey;

  do {
    const scanCommand = new ScanCommand({
      TableName: tableName,
      ExclusiveStartKey: exclusiveStartKey,
      ...options
    });

    // eslint-disable-next-line no-await-in-loop
    const scanResponse = await documentClient.send(scanCommand);

    exclusiveStartKey = scanResponse.LastEvaluatedKey;
    items.push(...scanResponse.Items);
  } while (exclusiveStartKey);

  return items;
}

module.exports = scan;
