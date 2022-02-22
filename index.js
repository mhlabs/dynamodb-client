const { DynamoDB } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocument } = require('@aws-sdk/lib-dynamodb');

const getItem = require('./dynamodb/get-item');

const client = new DynamoDB({});
const documentClient = DynamoDBDocument.from(client);

const dynamoClient = {
  getItem: (tableName, key) => getItem(documentClient, tableName, key)
};

module.exports = dynamoClient;
