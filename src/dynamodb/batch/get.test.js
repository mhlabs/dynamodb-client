jest.mock('./execute');

const execute = require('./execute');
const tested = require('./get');
const constants = require('./constants');

beforeEach(() => {
  jest.resetAllMocks();
  jest.restoreAllMocks();
  execute.mockResolvedValue([]);
});

describe('batch get', () => {
  it('should validate document client', async () => {
    await expect(tested(null, '', {})).rejects.toThrow(
      'documentClient is required.'
    );
  });

  it('should validate table', async () => {
    await expect(tested({}, '', {})).rejects.toThrow('tableName is required.');
  });

  it('should validate key', async () => {
    await expect(tested({}, 'table')).rejects.toThrow('Key list is required.');
  });

  it('should validate that keys are objects', async () => {
    const keys = [{}, 'b'];
    await expect(tested({}, 'table', keys)).rejects.toThrow(
      'All keys should be objects.'
    );
  });

  it('should split keys into chunks of max batch size (batchWrite limit)', async () => {
    const keys = Array(constants.MAX_KEYS_PER_BATCH_GET * 2 + 10).fill({
      id: 1
    });

    const res = await tested({}, 'testTable', keys);

    expect(execute).toHaveBeenCalledTimes(3);

    const firstRequest = execute.mock.calls[0][2].input.RequestItems.testTable;
    expect(firstRequest.Keys).toHaveLength(constants.MAX_KEYS_PER_BATCH_GET);

    const secondRequest = execute.mock.calls[1][2].input.RequestItems.testTable;
    expect(secondRequest.Keys).toHaveLength(constants.MAX_KEYS_PER_BATCH_GET);

    const thirdRequest = execute.mock.calls[2][2].input.RequestItems.testTable;
    expect(thirdRequest.Keys).toHaveLength(10);

    expect(res).toEqual([]);
  });
});
