import { WithXrayTraceId } from '../types';

export function removeXrayTraceId<T>(item: T): T {
  if (!('_xray_trace_id' in item)) return item;

  const { _xray_trace_id, ...restOfItem } = item as WithXrayTraceId<T>;
  return restOfItem as T;
}
