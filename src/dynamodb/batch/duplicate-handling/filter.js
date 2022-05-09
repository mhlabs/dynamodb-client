function filterUniqueKeys(keys = []) {
  if (!keys || !keys.length) return [];
  if (keys.some((key) => typeof key !== 'object'))
    throw new Error('All keys should must be objects.');

  const stringifiedKeys = [];
  const uniqueKeys = [];

  keys.forEach((key) => {
    if (
      !stringifiedKeys.find((stringKey) => stringKey === JSON.stringify(key))
    ) {
      stringifiedKeys.push(JSON.stringify(key));
      uniqueKeys.push(key);
    }
  });

  return uniqueKeys;
}

module.exports = {
  filterUniqueKeys
};
