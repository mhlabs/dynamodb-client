import { MhDynamoMiddlewareAfter, MhDynamoMiddlewareBefore } from './types';

export class MhDynamoMiddleware {
  protected middlewares: {
    before: MhDynamoMiddlewareBefore[];
    after: MhDynamoMiddlewareAfter[];
  } = { before: [], after: [] };

  public useBefore(middleware: MhDynamoMiddlewareBefore): this {
    if (this.middlewares.before.find((x) => x.id == middleware.id)) return this;

    this.middlewares.before.push(middleware);
    return this;
  }

  public useAfter(middleware: MhDynamoMiddlewareAfter): this {
    if (this.middlewares.after.find((x) => x.id == middleware.id)) return this;

    this.middlewares.after.push(middleware);
    return this;
  }

  protected runAfterMiddlewares<T>(item: T): T {
    if (!item) return item;

    this.middlewares.after.forEach((middleware) => {
      item = middleware.runAfter(item);
    });

    return item;
  }
}
