import { constants } from './constants';
import { parseRetryOptions as tested } from './retry-options';

describe('retry options', () => {
  it('should parse valid options', () => {
    const result = tested(0, 10);
    expect(result.minMs).toBe(0);
    expect(result.maxMs).toBe(10);
  });
  it('should use defaults if no valid options', () => {
    const result = tested(undefined, undefined);
    expect(result.minMs).toBe(
      constants.DEFAULT_UNPROCESSED_ITEMS_MIN_RETRY_TIMOUT_MS
    );
    expect(result.maxMs).toBe(
      constants.DEFAULT_UNPROCESSED_ITEMS_MAX_RETRY_TIMOUT_MS
    );
  });
});
