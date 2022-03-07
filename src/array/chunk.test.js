const { chunk } = require('./chunk');

describe('chunk', () => {
  it('should chunk items as configured', () => {
    const res = chunk([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 2);
    expect(res.length).toEqual(5);
    expect(res).toEqual([
      [1, 2],
      [3, 4],
      [5, 6],
      [7, 8],
      [9, 10]
    ]);
  });

  describe('validation', () => {
    it('should throw error if items is not an array', () => {
      const res = () =>
        chunk({ 1: 'asdf', 2: 'asdf', 3: 'asdf', 4: 'asdf' }, 2);

      expect(res).toThrow('Can only chunk arrays');
    });

    it('should throw error if itemsPerChunk is invalid', () => {
      const whenUndefined = () => chunk([]);

      expect(whenUndefined).toThrow('Must specify itemsPerChunk');

      const whenNotNumber = () => chunk([], '1');

      expect(whenNotNumber).toThrow('Must specify itemsPerChunk');
    });
  });
});
