// this file will be removed later on, used just to verify that gh actions work
import {
  CreateTableCommand,
  DeleteTableCommand,
  DynamoDBClient,
} from "@aws-sdk/client-dynamodb";

import { GetCommand, PutCommand } from "@aws-sdk/lib-dynamodb";

import { afterAll, beforeAll, expect, it } from "vitest";

const TableName = "table";

const dynamoClient = new DynamoDBClient({
  endpoint: "http://localhost:8000",
  credentials: {
    accessKeyId: "xxx",
    secretAccessKey: "yyy",
  },
});

const tableParams = {
  AttributeDefinitions: [
    {
      AttributeName: "PK",
      AttributeType: "S",
    },
  ],
  KeySchema: [
    {
      AttributeName: "PK",
      KeyType: "HASH",
    },
  ],
  ProvisionedThroughput: {
    ReadCapacityUnits: 1,
    WriteCapacityUnits: 1,
  },
  TableName,
  StreamSpecification: {
    StreamEnabled: false,
  },
};

beforeAll(async () => {
  await dynamoClient.send(new CreateTableCommand(tableParams));
});

afterAll(async () => {
  await dynamoClient.send(new DeleteTableCommand({ TableName }));
});

it("should be able to write to and read from table", async () => {
  const PK = "x";

  await dynamoClient.send(new PutCommand({ TableName, Item: { PK } }));

  const output = await dynamoClient.send(
    new GetCommand({ TableName, Key: { PK } })
  );

  expect(output.Item.PK).toBe(PK);
});
