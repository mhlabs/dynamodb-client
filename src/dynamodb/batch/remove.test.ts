import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb';
import { MhDynamoClient } from '../..';
import { constants } from './constants';
import { execute } from './execute';

jest.mock('./execute');
const executeMock = jest.mocked(execute);

let client: MhDynamoClient;

beforeEach(() => {
  jest.resetAllMocks();
  jest.restoreAllMocks();
  client = MhDynamoClient.fromDocumentClient({} as unknown as DynamoDBDocument);
});

describe('batch remove', () => {
  it('should split keys into chunks of max batch size (batchWrite limit)', async () => {
    const arrayLength = constants.MAX_ITEMS_PER_BATCH_WRITE * 2 + 10;
    const keys = Array.from(Array(arrayLength), (_, index) => ({
      id: index + 1
    }));

    const result = await client.batchRemove({
      tableName: 'testTable',
      keys
    });

    expect(executeMock).toHaveBeenCalledTimes(3);

    const firstRequest = executeMock.mock.calls[0][1].batchCommand.input
      .RequestItems?.testTable as any;
    expect(firstRequest).toHaveLength(constants.MAX_ITEMS_PER_BATCH_WRITE);
    expect(firstRequest.every((item) => item.DeleteRequest)).toBeTruthy();

    const secondRequest = executeMock.mock.calls[1][1].batchCommand.input
      .RequestItems?.testTable as any;
    expect(secondRequest).toHaveLength(constants.MAX_ITEMS_PER_BATCH_WRITE);
    expect(secondRequest.every((item) => item.DeleteRequest)).toBeTruthy();

    const thirdRequest = executeMock.mock.calls[2][1].batchCommand.input
      .RequestItems?.testTable as any;
    expect(thirdRequest).toHaveLength(10);
    expect(thirdRequest.every((item) => item.DeleteRequest)).toBeTruthy();

    expect(result).toBe(true);
  });
});
