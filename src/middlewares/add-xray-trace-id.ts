import { WithXrayTraceId } from '../types';

export const _X_AMZN_TRACE_ID = '_X_AMZN_TRACE_ID';

export function addXrayTraceId<T>(
  item: T,
  injectXrayTrace?: boolean
): WithXrayTraceId<T> | T {
  if (!injectXrayTrace) return item;

  const _xray_trace_id = getXrayTraceId();
  if (!_xray_trace_id) return item;

  return {
    ...item,
    _xray_trace_id
  };
}

export function getXrayTraceId() {
  return process.env[_X_AMZN_TRACE_ID]?.trim();
}
