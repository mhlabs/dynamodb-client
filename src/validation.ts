import { isMultidimensional } from './array/isMultidimensional';
import { BaseOptions } from './types';

export function ensureValidBase(options: BaseOptions) {
  if (!options.tableName) throw new Error('tableName is required.');
}

export function ensureValid(
  options: BaseOptions,
  object: Record<string, any>,
  objectName = 'object'
) {
  ensureValidBase(options);

  if (!object) throw new Error('object is required.');
  if (typeof object !== 'object')
    throw new Error(`${objectName} should be an object.`);
}

export function ensureValidQuery(
  options: BaseOptions,
  object: Record<string, any>,
  indexName: string
) {
  ensureValid(options, object, 'keyCondition');
  if (!indexName) throw new Error('indexName is required.');
}

export function ensureValidBatchWrite(
  options: BaseOptions,
  items: Record<string, any>[]
) {
  ensureValidBase(options);

  if (!items) throw new Error('Item list is required.');
  if (isMultidimensional(items)) {
    throw new Error("Item list can't contain arrays (be multidimensional).");
  }
}

export function ensureValidBatch(
  options: BaseOptions,
  keys: Record<string, any>[]
) {
  ensureValidBase(options);

  if (!keys) throw new Error('Key list is required.');
  if (!keys.every((key) => typeof key === 'object')) {
    throw new Error('Keys must be objects.');
  }
}
