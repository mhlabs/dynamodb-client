jest.mock('./execute');

import { execute } from './execute';
import { batchGet as tested } from './get';
import { constants } from './constants';

const executeMock = jest.mocked(execute);

beforeEach(() => {
  jest.resetAllMocks();
  jest.restoreAllMocks();
  executeMock.mockResolvedValue([]);
});

describe('batch get', () => {
  it('should split keys into chunks of max batch size (batchWrite limit)', async () => {
    const arrayLength = constants.MAX_KEYS_PER_BATCH_GET * 2 + 10;
    const keys = Array.from(Array(arrayLength), (_, index) => ({
      id: index + 1
    }));

    const res = await tested({} as any, 'testTable', keys);

    expect(executeMock).toHaveBeenCalledTimes(3);

    const firstRequest = executeMock.mock.calls[0][2].input.RequestItems
      ?.testTable as any;
    expect(firstRequest.Keys).toHaveLength(constants.MAX_KEYS_PER_BATCH_GET);

    const secondRequest = executeMock.mock.calls[1][2].input.RequestItems
      ?.testTable as any;
    expect(secondRequest.Keys).toHaveLength(constants.MAX_KEYS_PER_BATCH_GET);

    const thirdRequest = executeMock.mock.calls[2][2].input.RequestItems
      ?.testTable as any;
    expect(thirdRequest.Keys).toHaveLength(10);

    expect(res).toEqual([]);
  });
});
