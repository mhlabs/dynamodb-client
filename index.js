const { DynamoDB } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocument } = require('@aws-sdk/lib-dynamodb');

const batchGet = require('./src/dynamodb/batch/get');
const batchRemove = require('./src/dynamodb/batch/remove');
const batchWrite = require('./src/dynamodb/batch/write');
const getItem = require('./src/dynamodb/get-item');
const putItem = require('./src/dynamodb/put-item');
const remove = require('./src/dynamodb/remove');
const scan = require('./src/dynamodb/scan');

function createDynamoClient(documentClient) {
  return {
    batchGet: (
      tableName,
      keys,
      options,
      retryTimeoutMinMs,
      retryTimeoutMaxMs
    ) =>
      batchGet(
        documentClient,
        tableName,
        keys,
        options,
        retryTimeoutMinMs,
        retryTimeoutMaxMs
      ),
    batchRemove: (tableName, keys, retryTimeoutMinMs, retryTimeoutMaxMs) =>
      batchRemove(
        documentClient,
        tableName,
        keys,
        retryTimeoutMinMs,
        retryTimeoutMaxMs
      ),
    batchWrite: (tableName, items, retryTimeoutMinMs, retryTimeoutMaxMs) =>
      batchWrite(
        documentClient,
        tableName,
        items,
        retryTimeoutMinMs,
        retryTimeoutMaxMs
      ),
    getItem: (tableName, key, options) =>
      getItem(documentClient, tableName, key, options),
    putItem: (tableName, item, options) =>
      putItem(documentClient, tableName, item, options),
    remove: (tableName, key, options) =>
      remove(documentClient, tableName, key, options),
    scan: (tableName, options) => scan(documentClient, tableName, options)
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
