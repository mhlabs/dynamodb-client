import { WithLastModified } from "../types";

export function addLastModified<T>(item: T): WithLastModified<T> {
    return {
        ...item,
        _last_modified: new Date()
    };
}
