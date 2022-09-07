import { MhDynamoClient } from '..';
import { WithXrayTraceId } from '../types';

export function removeXrayTraceId<T>(
  this: MhDynamoClient,
  item: T,
  extractXrayTrace?: boolean
): T {
  if (!extractXrayTrace) return item;
  if (!('_xray_trace_id' in item)) return item;

  const { _xray_trace_id, ...restOfItem } = item as WithXrayTraceId<T>;
  return restOfItem as T;
}
