const {
  DynamoDBClient,
  CreateTableCommand,
  DeleteTableCommand
} = require('@aws-sdk/client-dynamodb');

const documentClient = require('../index');

const tableName = 'table';

const dynamoClient = new DynamoDBClient({
  endpoint: 'http://localhost:8000',
  credentials: {
    accessKeyId: 'xxx',
    secretAccessKey: 'yyy'
  }
});

const tableParams = {
  AttributeDefinitions: [
    {
      AttributeName: 'id',
      AttributeType: 'S'
    },
    {
      AttributeName: 'postal',
      AttributeType: 'N'
    }
  ],
  KeySchema: [
    {
      AttributeName: 'id',
      KeyType: 'HASH'
    }
  ],
  GlobalSecondaryIndexes: [
    {
      IndexName: 'gsi',
      KeySchema: [
        {
          AttributeName: 'postal',
          KeyType: 'HASH'
        }
      ],
      ProvisionedThroughput: {
        ReadCapacityUnits: 1,
        WriteCapacityUnits: 1
      },
      Projection: {
        ProjectionType: 'ALL'
      }
    }
  ],
  ProvisionedThroughput: {
    ReadCapacityUnits: 1,
    WriteCapacityUnits: 1
  },
  TableName: tableName,
  StreamSpecification: {
    StreamEnabled: false
  }
};

const duplicateOptions = {
  duplicateConfig: {
    partitionKeyAttributeName: 'id'
  }
};

beforeAll(async () => {
  await dynamoClient.send(new CreateTableCommand(tableParams));
});

afterAll(async () => {
  await dynamoClient.send(new DeleteTableCommand({ TableName: tableName }));
});

describe('dynamo integration tests', () => {
  const items = [
    { id: 'a', city: 'astad', postal: 1 },
    { id: 'b', city: 'bstad', postal: 2 }
  ];

  const dynamo = documentClient.init({
    endpoint: 'http://localhost:8000',
    credentials: {
      accessKeyId: 'xxx',
      secretAccessKey: 'yyy'
    }
  });

  it('should be able to perform all dynamo operations', async () => {
    let result = await dynamo.batchWrite(tableName, items, duplicateOptions);
    expect(result).toBe(true);

    result = await dynamo.scan(tableName);
    expect(result).toHaveLength(2);

    result = await dynamo.batchRemove(
      tableName,
      items.map((i) => ({ id: i.id }))
    );

    expect(result).toBe(true);

    result = await dynamo.scan(tableName);
    expect(result).toHaveLength(0);

    result = await dynamo.batchWrite(tableName, items, duplicateOptions);
    expect(result).toBe(true);

    result = await dynamo.batchGet(
      tableName,
      items.map((i) => ({ id: i.id }))
    );

    expect(result).toHaveLength(2);

    result = await dynamo.getItem(tableName, { id: items[1].id });
    expect(result.city).toBe(items[1].city);

    result = await dynamo.putItem(tableName, {
      id: 'c',
      city: 'cstad',
      postal: 3
    });
    expect(result).toBe(true);

    result = await dynamo.scan(tableName);
    expect(result).toHaveLength(3);

    result = await dynamo.query(tableName, { id: items[1].id });
    expect(result[0].city).toBe(items[1].city);

    result = await dynamo.queryByIndex(
      tableName,
      { postal: items[1].postal },
      tableParams.GlobalSecondaryIndexes[0].IndexName
    );
    expect(result[0].city).toBe(items[1].city);
  });
});
