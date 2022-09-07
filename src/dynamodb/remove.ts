import { DeleteCommand, DeleteCommandInput } from '@aws-sdk/lib-dynamodb';
import { MhDynamoClient } from '../../index';
import { BaseOptions, SingleItemOptions } from '../../types';

export interface RemoveOptions
  extends BaseOptions,
    Omit<SingleItemOptions, 'item'> {
  commandOptions?: DeleteCommandInput;
}

export async function remove(
  this: MhDynamoClient,
  options: RemoveOptions
): Promise<boolean> {
  options = this.mergeWithGlobalOptions(options);
  this.ensureValid(options, options.key, 'key');

  const cmdInput: DeleteCommandInput = {
    TableName: options.tableName,
    Key: options.key,
    ...options.commandOptions
  };

  const deleteCommand = new DeleteCommand(cmdInput);
  await this.documentClient.send(deleteCommand);

  return true;
}
