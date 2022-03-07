function chunk(items, itemsPerChunk) {
  if (!Array.isArray(items)) {
    throw new Error('Can only chunk arrays');
  }
  if (!itemsPerChunk || !Number.isInteger(itemsPerChunk)) {
    throw new Error('Must specify itemsPerChunk');
  }

  const itemsCopy = [...items];
  const chunkedItems = [];

  while (itemsCopy.length) {
    chunkedItems.push(itemsCopy.splice(0, itemsPerChunk));
  }

  return chunkedItems;
}

module.exports = {
  chunk
};
