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

const defaultOptions = {
  duplicateConfig: {
    partitionKeyAttributeName: '',
    sortKeyAttributeName: '',
    timestampAttributeName: ''
  }
};

function keyComparer(object1, object2, options) {
  const pk = options.duplicateConfig.partitionKeyAttributeName;
  const sk = options.duplicateConfig.sortKeyAttributeName;

  return object1[pk] === object2[pk] && object1[sk] === object2[sk];
}

function objectAlreadySetAsUnique(uniqueObjects, object, options) {
  return uniqueObjects.some((unique) => keyComparer(object, unique, options));
}

function getSortAttributeValues(a, b, sortAttribute) {
  if (!sortAttribute.includes('.'))
    return {
      a: a[sortAttribute],
      b: b[sortAttribute]
    };

  const firstSeparator = sortAttribute.indexOf('.');
  const currentAttribute = sortAttribute.substr(0, firstSeparator);
  const remainingAttributes = sortAttribute.substr(firstSeparator + 1);

  return getSortAttributeValues(
    a[currentAttribute],
    b[currentAttribute],
    remainingAttributes
  );
}

function getLatestInstance(objects, currentObject, options) {
  const sortAttribute = options.duplicateConfig.timestampAttributeName;

  const instances = objects.filter((object) =>
    keyComparer(currentObject, object, options)
  );

  if (instances.length === 1) return instances[0];
  if (!sortAttribute) return instances[instances.length - 1];

  const latest = instances.sort((a, b) => {
    const values = getSortAttributeValues(a, b, sortAttribute);
    return values.b >= values.a ? 1 : -1;
  });

  return latest[0];
}

function filterUniqueObjects(objects = [], options = defaultOptions) {
  if (
    !options.duplicateConfig.partitionKeyAttributeName ||
    !options.duplicateConfig.sortKeyAttributeName
  ) {
    throw new Error('Key attribute names must be set in duplicateConfig');
  }

  if (!objects.length) return objects;

  const uniqueObjects = [];

  objects.forEach((object) => {
    if (objectAlreadySetAsUnique(uniqueObjects, object, options)) return;

    const latest = getLatestInstance(objects, object, options);
    uniqueObjects.push(latest);
  });

  return uniqueObjects;
}

module.exports = {
  defaultOptions,
  filterUniqueKeys,
  filterUniqueObjects
};
