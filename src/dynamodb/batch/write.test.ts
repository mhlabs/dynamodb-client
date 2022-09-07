import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb';
import { MhDynamoClient } from '../..';
import { constants } from './constants';

const executeMock = jest.fn();
let client: MhDynamoClient;

beforeEach(() => {
  jest.resetAllMocks();
  jest.restoreAllMocks();
  client = MhDynamoClient.fromDocumentClient({} as unknown as DynamoDBDocument);
  client.execute = executeMock;
});

describe('batchWrite', () => {
  it('should split items into chunks of max batch size (batchWrite limit)', async () => {
    const arrayLength = constants.MAX_ITEMS_PER_BATCH_WRITE * 2 + 10;
    const items = Array.from(Array(arrayLength), (_, index) => ({
      id: index + 1
    }));

    const result = await client.batchWrite({
      tableName: 'testTable',
      items,
      options: {
        duplicateConfig: {
          partitionKeyAttributeName: 'id'
        }
      }
    });

    expect(executeMock).toHaveBeenCalledTimes(3);

    const firstRequest =
      executeMock.mock.calls[0][0].batchCommand.input.RequestItems?.testTable;
    expect(firstRequest).toHaveLength(constants.MAX_ITEMS_PER_BATCH_WRITE);

    const secondRequest =
      executeMock.mock.calls[1][0].batchCommand.input.RequestItems?.testTable;
    expect(secondRequest).toHaveLength(constants.MAX_ITEMS_PER_BATCH_WRITE);

    const thirdRequest =
      executeMock.mock.calls[2][0].batchCommand.input.RequestItems?.testTable;
    expect(thirdRequest).toHaveLength(10);

    expect(result).toBe(true);
  });
});