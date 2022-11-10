import { TranslateConfig } from '@aws-sdk/lib-dynamodb';

export interface MhDynamoClientOptions {
  translateConfig?: TranslateConfig;
  tableName?: string;
  injectXrayTrace?: boolean;
  extractXrayTrace?: boolean;
  injectLastModifiedAt?: boolean;
  extractLastModifiedAt?: boolean;
}

export interface BaseOptions {
  tableName?: string;
}

export interface BaseSaveOptions extends BaseOptions {
  injectXrayTrace?: boolean;
  injectLastModifiedAt?: boolean;
}

export interface BaseFetchOptions extends BaseOptions {
  extractXrayTrace?: boolean;
  extractLastModifiedAt?: boolean;
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

export type WithLastModifiedAt<T> = T & { _last_modified_at: Date };
