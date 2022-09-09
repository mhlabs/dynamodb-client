import {
  CreateTableCommand,
  DeleteTableCommand,
  DynamoDBClient
} from '@aws-sdk/client-dynamodb';

import { MhDynamoClient } from '../src/index';

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

interface DynamoItem {
  id: string;
  city: string;
  postal: number;
}

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

  const dynamo = MhDynamoClient.fromConfig({
    endpoint: 'http://localhost:8000',
    credentials: {
      accessKeyId: 'xxx',
      secretAccessKey: 'yyy'
    }
  });

  it('should be able to perform all dynamo operations', async () => {
    const result1 = await dynamo.batchWrite({
      items,
      tableName,
      options: duplicateOptions
    });
    expect(result1).toBe(true);

    const result2 = await dynamo.scan<DynamoItem>({ tableName });
    expect(result2).toHaveLength(2);

    const result3 = await dynamo.batchRemove({
      tableName,
      keys: items.map((i) => ({ id: i.id }))
    });

    expect(result3).toBe(true);

    const result4 = await dynamo.scan<DynamoItem>({ tableName });
    expect(result4).toHaveLength(0);

    const result5 = await dynamo.batchWrite({
      tableName,
      items,
      options: duplicateOptions
    });
    expect(result5).toBe(true);

    const result6 = await dynamo.batchGet<DynamoItem>({
      tableName,
      keys: items.map((i) => ({ id: i.id }))
    });

    expect(result6).toHaveLength(2);

    const result7 = await dynamo.getItem<DynamoItem>({
      tableName,
      key: { id: items[1].id }
    });
    expect(result7?.city).toBe(items[1].city);

    const result8 = await dynamo.putItem({
      tableName,
      item: {
        id: 'c',
        city: 'cstad',
        postal: 3
      }
    });
    expect(result8).toBe(true);

    const result9 = await dynamo.scan({ tableName });
    expect(result9).toHaveLength(3);

    const result10 = await dynamo.query<DynamoItem>({
      tableName,
      keyCondition: { id: items[1].id }
    });
    expect(result10[0].city).toBe(items[1].city);

    const result11 = await dynamo.queryByIndex<DynamoItem>({
      tableName,
      keyCondition: { postal: items[1].postal },
      indexName: tableParams.GlobalSecondaryIndexes[0].IndexName
    });
    expect(result11[0].city).toBe(items[1].city);
  });
});
