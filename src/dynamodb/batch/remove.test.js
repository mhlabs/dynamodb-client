jest.mock('./execute');

const execute = require('./execute');
const tested = require('./remove');
const constants = require('./constants');

beforeEach(() => {
  jest.resetAllMocks();
  jest.restoreAllMocks();
});

describe('batch delete', () => {
  it('should validate document client', async () => {
    await expect(tested(null, '', {})).rejects.toThrow(
      'documentClient is required.'
    );
  });

  it('should validate table', async () => {
    await expect(tested({}, '', {})).rejects.toThrow('Table name is required.');
  });

  it('should validate key', async () => {
    await expect(tested({}, 'table')).rejects.toThrow('Key list is required.');
  });

  it('should validate that keys are objects', async () => {
    const keys = [{}, 'b'];
    await expect(tested({}, 'table', keys)).rejects.toThrow(
      'Keys must be objects.'
    );
  });

  it('should split keys into chunks of max batch size (batchWrite limit)', async () => {
    const keys = Array(constants.MAX_ITEMS_PER_BATCH * 2 + 10).fill({ id: 1 });
    const res = await tested({}, 'testTable', keys);

    expect(execute).toHaveBeenCalledTimes(3);

    const firstRequest = execute.mock.calls[0][1].input.RequestItems.testTable;
    expect(firstRequest).toHaveLength(constants.MAX_ITEMS_PER_BATCH);
    expect(firstRequest.every((item) => item.DeleteRequest)).toBeTruthy();

    const secondRequest = execute.mock.calls[1][1].input.RequestItems.testTable;
    expect(secondRequest).toHaveLength(constants.MAX_ITEMS_PER_BATCH);
    expect(secondRequest.every((item) => item.DeleteRequest)).toBeTruthy();

    const thirdRequest = execute.mock.calls[2][1].input.RequestItems.testTable;
    expect(thirdRequest).toHaveLength(10);
    expect(thirdRequest.every((item) => item.DeleteRequest)).toBeTruthy();

    expect(res).toBe(true);
  });
});
