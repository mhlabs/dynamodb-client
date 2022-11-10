import { removeXrayTraceId } from './middlewares/remove-xray-trace-id';
import { BaseFetchOptions } from './types';
import { removeLastModified } from "./middlewares/remove-last-modified-at";

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

  if (options.extractXrayTrace) sanitized = removeXrayTraceId(sanitized);
  if (options.extractLastModified) sanitized = removeLastModified(sanitized);
  return sanitized;
}
