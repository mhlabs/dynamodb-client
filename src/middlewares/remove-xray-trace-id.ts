import { MhDynamoMiddlewareAfter, WithXrayTraceId } from '../types';

export class RemoveXrayTraceId implements MhDynamoMiddlewareAfter {
  public id: string = 'REMOVE_X_RAY_TRACE_ID';
  private xRayTraceIdPropertyName: string = '_xray_trace_id';

  constructor(xRayTraceIdPropertyName?: string) {
    if (xRayTraceIdPropertyName) {
      this.xRayTraceIdPropertyName = xRayTraceIdPropertyName;
    }
  }

  public runAfter<T>(item: any): T {
    if (!(this.xRayTraceIdPropertyName in item)) return item;

    const { _xray_trace_id, ...restOfItem } = item as WithXrayTraceId<T>;
    return restOfItem as T;
  }
}
