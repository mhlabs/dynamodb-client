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
  executeMock.mockResolvedValue([]);
  client = MhDynamoClient.fromDocumentClient({} as unknown as DynamoDBDocument);
});

describe('batchGet', () => {
  it('should split keys into chunks of max batch size (batchWrite limit)', async () => {
    const arrayLength = constants.MAX_KEYS_PER_BATCH_GET * 2 + 10;
    const keys = Array.from(Array(arrayLength), (_, index) => ({
      id: index + 1
    }));

    const result = await client.batchGet({
      tableName: 'testTable',
      keys
    });

    expect(executeMock).toHaveBeenCalledTimes(3);

    const firstRequest = executeMock.mock.calls[0][1].batchCommand.input
      .RequestItems?.testTable as any;
    expect(firstRequest.Keys).toHaveLength(constants.MAX_KEYS_PER_BATCH_GET);

    const secondRequest = executeMock.mock.calls[1][1].batchCommand.input
      .RequestItems?.testTable as any;
    expect(secondRequest.Keys).toHaveLength(constants.MAX_KEYS_PER_BATCH_GET);

    const thirdRequest = executeMock.mock.calls[2][1].batchCommand.input
      .RequestItems?.testTable as any;
    expect(thirdRequest.Keys).toHaveLength(10);

    expect(result).toEqual([]);
  });
});
