import { randomInteger } from './random-integer';

describe('random assignment', () => {
  it('should assign randomly and evenly', () => {
    const summary = {
      lessThan500: 0,
      atOrOver500: 0
    };

    for (let ix = 0; ix < 1000; ix += 1) {
      const r = randomInteger(1, 1000);

      if (r < 500) summary.lessThan500 += 1;
      else summary.atOrOver500 += 1;
    }

    expect(summary.lessThan500).toBeGreaterThan(400);
    expect(summary.atOrOver500).toBeGreaterThan(400);
  });

  it('should return given value if min and max are the same', () => {
    const r = randomInteger(0, 0);
    expect(r).toBe(0);
  });

  it('should return value in given interval', () => {
    const r = randomInteger(1, 3);
    expect(r).toBeGreaterThanOrEqual(1);
    expect(r).toBeLessThanOrEqual(3);
  });
});
