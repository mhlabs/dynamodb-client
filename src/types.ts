import { TranslateConfig } from '@aws-sdk/lib-dynamodb';

export interface MhDynamoClientOptions {
  translateConfig?: TranslateConfig;
  tableName?: string;
  injectXrayTrace?: boolean;
  extractXrayTrace?: boolean;
  injectLastModified?: boolean;
  extractLastModified?: boolean;
}

export interface BaseOptions {
  tableName?: string;
}

export interface BaseSaveOptions extends BaseOptions {
  injectXrayTrace?: boolean;
  injectLastModified?: boolean;
}

export interface BaseFetchOptions extends BaseOptions {
  extractXrayTrace?: boolean;
  extractLastModified?: boolean;
}

export interface BatchRetryOptions {
  retryTimeoutMinMs?: number;
  retryTimeoutMaxMs?: number;
}

export interface SingleItemOptions {
  key: Record<string, any>;
  item: Record<string, any>;
}

export interface MultiItemOptions {
  items: Record<string, any>[];
  keys: Record<string, any>[];
}

export type WithXrayTraceId<T> = T & { _xray_trace_id?: string };

export type WithLastModified<T> = T & { _last_modified: Date };
