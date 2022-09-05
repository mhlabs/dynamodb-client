jest.mock('./execute');

import { execute } from './execute';
import { batchWrite as tested } from './write';
import { constants } from './constants';

const executeMock = jest.mocked(execute);

beforeEach(() => {
  jest.resetAllMocks();
  jest.restoreAllMocks();
});

describe('batchWrite', () => {
  it('should split items into chunks of max batch size (batchWrite limit)', async () => {
    const arrayLength = constants.MAX_ITEMS_PER_BATCH_WRITE * 2 + 10;
    const items = Array.from(Array(arrayLength), (_, index) => ({
      id: index + 1
    }));

    const res = await tested({} as any, 'testTable', items, {
      duplicateConfig: {
        partitionKeyAttributeName: 'id'
      }
    });

    expect(executeMock).toHaveBeenCalledTimes(3);

    const firstRequest =
      executeMock.mock.calls[0][2].input.RequestItems?.testTable;
    expect(firstRequest).toHaveLength(constants.MAX_ITEMS_PER_BATCH_WRITE);

    const secondRequest =
      executeMock.mock.calls[1][2].input.RequestItems?.testTable;
    expect(secondRequest).toHaveLength(constants.MAX_ITEMS_PER_BATCH_WRITE);

    const thirdRequest =
      executeMock.mock.calls[2][2].input.RequestItems?.testTable;
    expect(thirdRequest).toHaveLength(10);

    expect(res).toBe(true);
  });
});
