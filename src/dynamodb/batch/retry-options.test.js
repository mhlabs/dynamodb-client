const constants = require('./constants');
const tested = require('./retry-options');

describe('retry options', () => {
  it('should parse valid options', () => {
    const result = tested(0, 10);
    expect(result.minMs).toBe(0);
    expect(result.maxMs).toBe(10);
  });
  it('should use defaults if no valid options', () => {
    const result = tested(undefined, 'x');
    expect(result.minMs).toBe(
      constants.DEFAULT_UNPROCESSED_MIN_RETRY_TIMOUT_MS
    );
    expect(result.maxMs).toBe(
      constants.DEFAULT_UNPROCESSED_MAX_RETRY_TIMOUT_MS
    );
  });
});
