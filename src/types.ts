import { GetCommandOutput } from '@aws-sdk/lib-dynamodb';
import { Merge } from 'type-fest';

export type WithXrayTraceId<T> = T & { _xray_trace_id?: string };

export interface MhDynamoMiddlewareBefore {
  id: string;
  runBefore<T>(item: any): T;
}

export interface MhDynamoMiddlewareAfter {
  id: string;
  runAfter<T>(item: any): T;
}

export type MhGetCommandOutput<T> = Merge<
  GetCommandOutput,
  {
    Item: T;
  }
>;
