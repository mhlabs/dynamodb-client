const constants = require('./constants');

function parseRetryOptions(retryTimeoutMinMs, retryTimeoutMaxMs) {
  return {
    minMs:
      retryTimeoutMinMs >= 0
        ? retryTimeoutMinMs
        : constants.DEFAULT_UNPROCESSED_ITEMS_MIN_RETRY_TIMOUT_MS,
    maxMs:
      retryTimeoutMaxMs >= 0
        ? retryTimeoutMaxMs
        : constants.DEFAULT_UNPROCESSED_ITEMS_MAX_RETRY_TIMOUT_MS
  };
}

module.exports = parseRetryOptions;
