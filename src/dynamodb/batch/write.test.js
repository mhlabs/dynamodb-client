jest.mock('./execute');

const execute = require('./execute');
const tested = require('./write');
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
    await expect(tested({}, 'table', [1, [2, 3]])).rejects.toThrow(
      'Item list should not be multidimensional.'
    );
  });

  it('should split items into chunks of max batch size (batchWrite limit)', async () => {
    const arrayLength = constants.MAX_ITEMS_PER_BATCH_WRITE * 2 + 10;
    const items = Array.from(Array(arrayLength), (_, index) => ({
      id: index + 1
    }));

    const res = await tested({}, 'testTable', items, {
      duplicateConfig: {
        partitionKeyAttributeName: 'id'
      }
    });

    expect(execute).toHaveBeenCalledTimes(3);

    const firstRequest = execute.mock.calls[0][2].input.RequestItems.testTable;
    expect(firstRequest).toHaveLength(constants.MAX_ITEMS_PER_BATCH_WRITE);

    const secondRequest = execute.mock.calls[1][2].input.RequestItems.testTable;
    expect(secondRequest).toHaveLength(constants.MAX_ITEMS_PER_BATCH_WRITE);

    const thirdRequest = execute.mock.calls[2][2].input.RequestItems.testTable;
    expect(thirdRequest).toHaveLength(10);

    expect(res).toBe(true);
  });
});
