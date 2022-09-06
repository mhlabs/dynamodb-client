import { DeleteCommand, DeleteCommandInput } from '@aws-sdk/lib-dynamodb';
import { BaseInput, MhDynamoClient, SingleItemInput } from '../../index';

export interface RemoveInput extends BaseInput, Omit<SingleItemInput, 'item'> {
  options?: DeleteCommandInput;
}

export async function remove(
  this: MhDynamoClient,
  input: RemoveInput
): Promise<boolean> {
  this.ensureValid(input.tableName, input.key, 'key');

  const cmdInput: DeleteCommandInput = {
    TableName: input.tableName,
    Key: input.key,
    ...input.options
  };

  const deleteCommand = new DeleteCommand(cmdInput);
  await this.documentClient.send(deleteCommand);

  return true;
}
