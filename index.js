const { DynamoDB } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocument } = require('@aws-sdk/lib-dynamodb');

const getItem = require('./dynamodb/get-item');

function createDynamoClient(documentClient) {
  return {
    getItem: (tableName, key, options) =>
      getItem(documentClient, tableName, key, options)
  };
}

function init(dynamoDbClientConfig = undefined, translateConfig = undefined) {
  const client = new DynamoDB(dynamoDbClientConfig);
  const documentClient = DynamoDBDocument.from(client, translateConfig);

  return createDynamoClient(documentClient);
}

const dynamoClient = {
  init
};

module.exports = dynamoClient;
