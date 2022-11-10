import { WithLastModified } from '../types';

export function removeLastModified<T>(item: T): T {
  if (!('_last_modified' in item)) return item;

  const { _last_modified, ...restOfItem } = item as WithLastModified<T>;
  return restOfItem as T;
}
