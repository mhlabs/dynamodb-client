const { DynamoDB } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocument } = require('@aws-sdk/lib-dynamodb');

const getItem = require('./src/dynamodb/get-item');
const batchWrite = require('./src/dynamodb/batch-write');

function createDynamoClient(documentClient) {
  return {
    batchWrite: (tableName, items, retryTimeoutMinMs, retryTimeoutMaxMs) =>
      batchWrite(
        documentClient,
        tableName,
        items,
        retryTimeoutMinMs,
        retryTimeoutMaxMs
      ),
    getItem: (tableName, key, options) =>
      getItem(documentClient, tableName, key, options)
  };
}

function init(dynamoDbClientConfig = undefined, translateConfig = undefined) {
  const client = new DynamoDB(dynamoDbClientConfig);

  const translateOptions = { ...translateConfig };

  if (!translateOptions.marshallOptions) {
    translateOptions.marshallOptions = {
      removeUndefinedValues: true
    };
  }

  const documentClient = DynamoDBDocument.from(client, translateOptions);

  return createDynamoClient(documentClient);
}

const dynamoClient = {
  init
};

module.exports = dynamoClient;
