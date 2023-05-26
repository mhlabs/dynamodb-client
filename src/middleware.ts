import {
  GetCommandInput,
  GetCommandOutput,
  PutCommandInput,
  PutCommandOutput,
} from '@aws-sdk/lib-dynamodb';
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

  protected runAfterMiddlewares<T extends GetCommandOutput | PutCommandOutput>(
    commandOutput: T
  ): T {
    if (!commandOutput) return commandOutput;

    this.middlewares.after.forEach((middleware) => {
      commandOutput = middleware.runAfter(commandOutput);
    });

    return commandOutput;
  }

  protected runBeforeMiddlewares<T extends GetCommandInput | PutCommandInput>(
    commandInput: T
  ): T {
    if (!commandInput) return commandInput;

    this.middlewares.before.forEach((middleware) => {
      commandInput = middleware.runBefore(commandInput);
    });

    return commandInput;
  }
}
