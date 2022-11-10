import { WithLastModifiedAt } from "../types";

export function addLastModifiedAt<T>(item: T): WithLastModifiedAt<T> {
    return {
        ...item,
        _last_modified_at: new Date()
    };
}
