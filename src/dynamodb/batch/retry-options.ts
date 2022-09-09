import { constants } from './constants';

export interface RetryOptions {
  minMs: number;
  maxMs: number;
}

export const parseRetryOptions = (
  retryTimeoutMinMs?: number,
  retryTimeoutMaxMs?: number
): RetryOptions => {
  return {
    minMs:
      retryTimeoutMinMs !== undefined && retryTimeoutMinMs >= 0
        ? retryTimeoutMinMs
        : constants.DEFAULT_UNPROCESSED_ITEMS_MIN_RETRY_TIMOUT_MS,
    maxMs:
      retryTimeoutMaxMs !== undefined && retryTimeoutMaxMs >= 0
        ? retryTimeoutMaxMs
        : constants.DEFAULT_UNPROCESSED_ITEMS_MAX_RETRY_TIMOUT_MS
  };
};
