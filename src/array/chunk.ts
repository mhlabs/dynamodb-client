export const chunk = <T>(items: T[], itemsPerChunk: number): T[][] => {
  if (!Array.isArray(items)) {
    throw new Error('Can only chunk arrays');
  }
  if (!itemsPerChunk || !Number.isInteger(itemsPerChunk)) {
    throw new Error('Must specify itemsPerChunk');
  }

  const itemsCopy = [...items];
  const chunkedItems: T[][] = [];

  while (itemsCopy.length) {
    chunkedItems.push(itemsCopy.splice(0, itemsPerChunk));
  }

  return chunkedItems;
};
