import { DynamoDB } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocument, GetCommand } from '@aws-sdk/lib-dynamodb';
import { MhDynamoClient } from '.';

import { mockClient } from 'aws-sdk-client-mock';

const dynamoDbDocumentMock = mockClient(DynamoDBDocument);

interface DynamoItem {
  id: number;
}

beforeEach(() => {
  dynamoDbDocumentMock.reset();
  dynamoDbDocumentMock
    .on(GetCommand, {
      ['Key' as string]: { id: 15 }
    })
    .resolves({ Item: { id: 15, _xray_trace_id: 'trace' } });
});

describe('index', () => {
  describe('init fromConfig', () => {
    it('should work', () => {
      const config = {};
      const client = MhDynamoClient.fromConfig(config);
      expect(client).not.toBeFalsy();
    });

    it('should work without parameters', () => {
      const client = MhDynamoClient.fromConfig();
      expect(client).not.toBeFalsy();
    });

    it('should be able to use methods', async () => {
      const client = MhDynamoClient.fromConfig(undefined, {
        tableName: 'table'
      });
      const res = await client.getItem<DynamoItem>({ key: { id: 15 } });

      expect(dynamoDbDocumentMock.commandCalls(GetCommand)).toHaveLength(1);
      expect(res).toEqual({
        id: 15
      });
    });
  });

  describe('init fromClient', () => {
    it('should work', () => {
      const dynamoClient = new DynamoDB({});
      const client = MhDynamoClient.fromClient(dynamoClient);
      expect(client).not.toBeFalsy();
    });

    it('should be able to use methods', async () => {
      const dynamoClient = new DynamoDB({});
      const client = MhDynamoClient.fromClient(dynamoClient, {
        tableName: 'table'
      });
      const res = await client.getItem<DynamoItem>({ key: { id: 15 } });

      expect(dynamoDbDocumentMock.commandCalls(GetCommand)).toHaveLength(1);
      expect(res).toEqual({
        id: 15
      });
    });
  });

  describe('init fromDocumentClient', () => {
    it('should work', () => {
      const dynamoClient = new DynamoDB({});
      const documentClient = DynamoDBDocument.from(dynamoClient);
      const client = MhDynamoClient.fromDocumentClient(documentClient);
      expect(client).not.toBeFalsy();
    });

    it('should be able to use methods', async () => {
      const client = MhDynamoClient.fromDocumentClient(
        dynamoDbDocumentMock as unknown as DynamoDBDocument,
        {
          tableName: 'table'
        }
      );
      const res = await client.getItem<DynamoItem>({ key: { id: 15 } });

      expect(dynamoDbDocumentMock.commandCalls(GetCommand)).toHaveLength(1);
      expect(res).toEqual({
        id: 15
      });
    });
  });
});
