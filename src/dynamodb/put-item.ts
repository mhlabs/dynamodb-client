import { PutCommand, PutCommandInput } from '@aws-sdk/lib-dynamodb';
import { enrichInput } from '../enrich';
import { MhDynamoClient } from '../index';
import { BaseSaveOptions, SingleItemOptions } from '../types';
import { ensureValid } from '../validation';

export interface PutOptions
  extends BaseSaveOptions,
    Omit<SingleItemOptions, 'key'> {
  commandOptions?: PutCommandInput;
}

export async function putItem(this: MhDynamoClient, options: PutOptions) {
  options = this.mergeWithGlobalOptions(options);
  ensureValid(options, options.item, 'item');

  options.item = enrichInput(options.item, options);

  const cmdInput: PutCommandInput = {
    TableName: options.tableName,
    Item: options.item,
    ...options.commandOptions
  };

  const command = new PutCommand(cmdInput);
  await this.documentClient.send(command);

  return true;
}
