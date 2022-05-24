const { isMultidimensional } = require('./isMultidimensional');

describe('isMultidimensional', () => {
  const isNotMultidimensionalCases = [[[1, 2]], [[]], [null]];
  it.each(isNotMultidimensionalCases)(
    'should not be multidimensional',
    (value) => {
      const res = isMultidimensional(value);
      expect(res).toEqual(false);
    }
  );

  it('should be multidimensional', () => {
    const res = isMultidimensional([1, [2, 3]]);
    expect(res).toEqual(true);
  });
});
