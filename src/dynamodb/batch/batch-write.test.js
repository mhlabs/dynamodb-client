jest.mock('./execute');

const execute = require('./execute');
const tested = require('./batch-write');
const constants = require('./constants');

beforeEach(() => {
  jest.resetAllMocks();
  jest.restoreAllMocks();
});

describe('batchWrite', () => {
  it('should validate document client', async () => {
    await expect(tested(null, '', {})).rejects.toThrow(
      'documentClient is required.'
    );
  });

  it('should validate table', async () => {
    await expect(tested({}, '', {})).rejects.toThrow('Table name is required.');
  });

  it('should validate item list', async () => {
    await expect(tested({}, 'table')).rejects.toThrow('Item list is required.');
  });

  it('should split items into chunks of max batch size (batchWrite limit)', async () => {
    const items = Array(constants.MAX_ITEMS_PER_BATCH * 2 + 10).fill({ id: 1 });
    const res = await tested({}, 'testTable', items);

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
