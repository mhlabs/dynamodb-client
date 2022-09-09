import { chunk } from './chunk';

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
});
