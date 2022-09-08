import { removeXrayTraceId } from './middlewares/remove-xray-trace-id';
import { BaseFetchOptions } from './types';

export function sanitizeOutputs<T>(
  output: T[],
  options: BaseFetchOptions
): T[] {
  return output.map((i) => {
    return sanitizeOutput(i, options);
  });
}

export function sanitizeOutput<T>(output: T, options: BaseFetchOptions): T {
  let sanitized = { ...output };

  sanitized = removeXrayTraceId(sanitized, options.extractXrayTrace);
  return sanitized;
}
