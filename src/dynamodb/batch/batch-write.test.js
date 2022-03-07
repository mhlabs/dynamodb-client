jest.mock('./execute');

const execute = require('./execute');
const tested = require('./batch-write');
const constants = require('./constants');

beforeEach(() => {
  jest.resetAllMocks();
  jest.restoreAllMocks();
});

describe('batchWrite', () => {
  it('should split items into chunks of max batch size (batchWrite limit)', async () => {
    const items = Array(constants.MAX_ITEMS_PER_BATCH * 2 + 10).fill({ id: 1 });
    const res = await tested(null, 'testTable', items);

    expect(execute).toHaveBeenCalledTimes(3);

    const firstRequest = execute.mock.calls[0][1].input.RequestItems.testTable;
    expect(firstRequest).toHaveLength(constants.MAX_ITEMS_PER_BATCH);

    const secondRequest = execute.mock.calls[1][1].input.RequestItems.testTable;
    expect(secondRequest).toHaveLength(constants.MAX_ITEMS_PER_BATCH);

    const thirdRequest = execute.mock.calls[2][1].input.RequestItems.testTable;
    expect(thirdRequest).toHaveLength(10);

    expect(res).toBe(true);
  });
});
