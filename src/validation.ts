import { MhDynamoClient } from '.';
import { isMultidimensional } from './array/isMultidimensional';
import { BaseOptions } from './types';

export function ensureValidBase(this: MhDynamoClient, options: BaseOptions) {
  if (!this.documentClient) throw new Error('documentClient is required.');
  if (!options.tableName) throw new Error('tableName is required.');
}

export function ensureValid(
  this: MhDynamoClient,
  options: BaseOptions,
  object: Record<string, any>,
  objectName = 'object'
) {
  this.ensureValidBase(options);

  if (!object) throw new Error('object is required.');
  if (typeof object !== 'object')
    throw new Error(`${objectName} should be an object.`);
}

export function ensureValidQuery(
  this: MhDynamoClient,
  options: BaseOptions,
  object: Record<string, any>,
  indexName: string
) {
  this.ensureValid(options, object, 'keyCondition');
  if (!indexName) throw new Error('indexName is required.');
}

export function ensureValidBatchWrite(
  this: MhDynamoClient,
  options: BaseOptions,
  items: Record<string, any>[]
) {
  this.ensureValidBase(options);

  if (!items) throw new Error('Item list is required.');
  if (isMultidimensional(items)) {
    throw new Error("Item list can't contain arrays (be multidimensional).");
  }
}

export function ensureValidBatch(
  this: MhDynamoClient,
  options: BaseOptions,
  keys: Record<string, any>[]
) {
  this.ensureValidBase(options);

  if (!keys) throw new Error('Key list is required.');
  if (!keys.every((key) => typeof key === 'object')) {
    throw new Error('Keys must be objects.');
  }
}
