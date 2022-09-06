import { ScanCommand, ScanCommandInput } from '@aws-sdk/lib-dynamodb';
import { BaseInput, MhDynamoClient } from '../../index';

export interface ScanInput extends BaseInput {
  options?: ScanCommandInput;
}

export async function scan<T>(
  this: MhDynamoClient,
  input: ScanInput
): Promise<T[]> {
  this.ensureValidBase(input.tableName);

  const items: T[] = [];
  let exclusiveStartKey: { [key: string]: any } | undefined;

  do {
    const cmdInput: ScanCommandInput = {
      TableName: input.tableName,
      ExclusiveStartKey: exclusiveStartKey,
      ...input.options
    };

    const scanCommand = new ScanCommand(cmdInput);
    // eslint-disable-next-line no-await-in-loop
    const scanResponse = await this.documentClient.send(scanCommand);

    exclusiveStartKey = scanResponse.LastEvaluatedKey;
    items.push(...(scanResponse.Items as T[]));
  } while (exclusiveStartKey);

  return items;
}
