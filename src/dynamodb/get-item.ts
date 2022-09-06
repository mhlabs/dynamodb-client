import { GetCommand, GetCommandInput } from '@aws-sdk/lib-dynamodb';
import { BaseInput, MhDynamoClient, SingleItemInput } from '../../index';

export interface GetInput extends BaseInput, Omit<SingleItemInput, 'item'> {
  options?: GetCommandInput;
}

export async function getItem<T>(
  this: MhDynamoClient,
  input: GetInput
): Promise<T | null> {
  this.ensureValid(input.tableName, input.key, 'key');

  const cmdInput: GetCommandInput = {
    TableName: input.tableName,
    Key: input.key,
    ...input.options
  };

  const command = new GetCommand(cmdInput);
  const response = await this.documentClient.send(command);

  return (response?.Item as T) || null;
}
