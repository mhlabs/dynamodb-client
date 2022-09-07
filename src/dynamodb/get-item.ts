import { GetCommand, GetCommandInput } from '@aws-sdk/lib-dynamodb';
import { MhDynamoClient } from '../index';
import { BaseFetchOptions, SingleItemOptions } from '../types';

export interface GetOptions
  extends BaseFetchOptions,
    Omit<SingleItemOptions, 'item'> {
  commandOptions?: GetCommandInput;
}

export async function getItem<T>(
  this: MhDynamoClient,
  options: GetOptions
): Promise<T | null> {
  this.ensureValid(options, options.key, 'key');

  const cmdInput: GetCommandInput = {
    TableName: options.tableName,
    Key: options.key,
    ...options.commandOptions
  };

  const command = new GetCommand(cmdInput);
  const response = await this.documentClient.send(command);

  return !response?.Item
    ? null
    : this.sanitizeOutput(response?.Item as T, options);
}
