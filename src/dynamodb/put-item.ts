import { PutCommand, PutCommandInput } from '@aws-sdk/lib-dynamodb';
import { BaseInput, MhDynamoClient, SingleItemInput } from '../../index';

export interface PutInput extends BaseInput, Omit<SingleItemInput, 'key'> {
  options?: PutCommandInput;
}

export async function putItem(this: MhDynamoClient, input: PutInput) {
  this.ensureValid(input.tableName, input.item, 'item');

  const cmdInput: PutCommandInput = {
    TableName: input.tableName,
    Item: input.item,
    ...input.options
  };

  const command = new PutCommand(cmdInput);
  await this.documentClient.send(command);

  return true;
}
