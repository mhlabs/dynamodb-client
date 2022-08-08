const { DynamoDB } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocument } = require('@aws-sdk/lib-dynamodb');

const batchGet = require('./src/dynamodb/batch/get');
const batchRemove = require('./src/dynamodb/batch/remove');
const batchWrite = require('./src/dynamodb/batch/write');
const getItem = require('./src/dynamodb/get-item');
const putItem = require('./src/dynamodb/put-item');
const remove = require('./src/dynamodb/remove');
const query = require('./src/dynamodb/query');
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
    batchRemove: (
      tableName,
      keys,
      options,
      retryTimeoutMinMs,
      retryTimeoutMaxMs
    ) =>
      batchRemove(
        documentClient,
        tableName,
        keys,
        options,
        retryTimeoutMinMs,
        retryTimeoutMaxMs
      ),
    batchWrite: (
      tableName,
      items,
      options,
      retryTimeoutMinMs,
      retryTimeoutMaxMs
    ) =>
      batchWrite(
        documentClient,
        tableName,
        items,
        options,
        retryTimeoutMinMs,
        retryTimeoutMaxMs
      ),
    getItem: (tableName, key, options) =>
      getItem(documentClient, tableName, key, options),
    putItem: (tableName, item, options) =>
      putItem(documentClient, tableName, item, options),
    query: (tableName, keyCondition) =>
      query(documentClient, tableName, keyCondition),
    queryByIndex: (tableName, keyCondition, indexName) =>
      query(documentClient, tableName, keyCondition, true, indexName),
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

function initWithClient(client, translateConfig = undefined) {
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
  init,
  initWithClient
};

module.exports = dynamoClient;
