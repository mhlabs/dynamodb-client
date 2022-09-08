import { ScanCommand, ScanCommandInput } from '@aws-sdk/lib-dynamodb';
import { MhDynamoClient } from '../index';
import { sanitizeOutputs } from '../sanitize';
import { BaseFetchOptions } from '../types';
import { ensureValidBase } from '../validation';

export interface ScanOptions extends BaseFetchOptions {
  commandOptions?: ScanCommandInput;
}

export async function scan<T>(
  this: MhDynamoClient,
  options: ScanOptions
): Promise<T[]> {
  options = this.mergeWithGlobalOptions<ScanOptions>(options);
  ensureValidBase(options);

  const items: T[] = [];
  let exclusiveStartKey: { [key: string]: any } | undefined;

  do {
    const cmdInput: ScanCommandInput = {
      TableName: options.tableName,
      ExclusiveStartKey: exclusiveStartKey,
      ...options.commandOptions
    };

    const scanCommand = new ScanCommand(cmdInput);
    // eslint-disable-next-line no-await-in-loop
    const scanResponse = await this.documentClient.send(scanCommand);

    exclusiveStartKey = scanResponse.LastEvaluatedKey;
    items.push(...(scanResponse.Items as T[]));
  } while (exclusiveStartKey);

  return sanitizeOutputs(items, options);
}
