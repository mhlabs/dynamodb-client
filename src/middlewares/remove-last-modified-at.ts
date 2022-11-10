import { WithLastModifiedAt } from '../types';

export function removeLastModifiedAt<T>(item: T): T {
  if (!('_last_modified_at' in item)) return item;

  const { _last_modified_at, ...restOfItem } = item as WithLastModifiedAt<T>;
  return restOfItem as T;
}
