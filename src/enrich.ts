import { addXrayTraceId } from './middlewares/add-xray-trace-id';
import { BaseSaveOptions } from './types';

export function enrichInputs<T>(input: T[], options: BaseSaveOptions): T[] {
  return input.map((i) => {
    return enrichInput(i, options);
  });
}

export function enrichInput<T>(input: T, options: BaseSaveOptions): T {
  let enriched = { ...input };

  if (options.injectXrayTrace) enriched = addXrayTraceId(enriched);
  return enriched;
}
