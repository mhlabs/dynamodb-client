import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb';

jest.mock('./execute');

import { execute } from './execute';
import { batchRemove as tested } from './remove';
import { constants } from './constants';

beforeEach(() => {
  jest.resetAllMocks();
  jest.restoreAllMocks();
});

describe('batch remove', () => {
  it('should split keys into chunks of max batch size (batchWrite limit)', async () => {
    const arrayLength = constants.MAX_ITEMS_PER_BATCH_WRITE * 2 + 10;
    const keys = Array.from(Array(arrayLength), (_, index) => ({
      id: index + 1
    }));

    const res = await tested({} as DynamoDBDocument, 'testTable', keys);

    expect(execute).toHaveBeenCalledTimes(3);

    const firstRequest = execute.mock.calls[0][2].input.RequestItems.testTable;
    expect(firstRequest).toHaveLength(constants.MAX_ITEMS_PER_BATCH_WRITE);
    expect(firstRequest.every((item) => item.DeleteRequest)).toBeTruthy();

    const secondRequest = execute.mock.calls[1][2].input.RequestItems.testTable;
    expect(secondRequest).toHaveLength(constants.MAX_ITEMS_PER_BATCH_WRITE);
    expect(secondRequest.every((item) => item.DeleteRequest)).toBeTruthy();

    const thirdRequest = execute.mock.calls[2][2].input.RequestItems.testTable;
    expect(thirdRequest).toHaveLength(10);
    expect(thirdRequest.every((item) => item.DeleteRequest)).toBeTruthy();

    expect(res).toBe(true);
  });
});
