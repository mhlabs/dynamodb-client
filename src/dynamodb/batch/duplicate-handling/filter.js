function filterUniqueKeys(keyObjects = []) {
  if (!keyObjects || !keyObjects.length) return [];
  if (keyObjects.some((key) => typeof key !== 'object'))
    throw new Error('All keys must be objects.');

  const stringifiedKeyObjects = [];
  const uniqueKeyObjects = [];

  keyObjects.forEach((keyObject) => {
    if (
      !stringifiedKeyObjects.find(
        (stringifiedKeyObject) =>
          stringifiedKeyObject === JSON.stringify(keyObject)
      )
    ) {
      stringifiedKeyObjects.push(JSON.stringify(keyObject));
      uniqueKeyObjects.push(keyObject);
    }
  });

  return uniqueKeyObjects;
}

const defaultDuplicateOptions = {
  duplicateConfig: {
    partitionKeyAttributeName: '',
    sortKeyAttributeName: '',
    timestampAttributeName: ''
  }
};

function keyComparer(object1, object2, options) {
  const pk = options.duplicateConfig.partitionKeyAttributeName;
  const sk = options.duplicateConfig.sortKeyAttributeName;

  if (sk) return object1[pk] === object2[pk] && object1[sk] === object2[sk];
  return object1[pk] === object2[pk];
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

function filterUniqueObjects(objects = [], options = defaultDuplicateOptions) {
  if (!options.duplicateConfig.partitionKeyAttributeName) {
    throw new Error(
      'At least partition key attribute must be set in duplicateConfig'
    );
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
  defaultDuplicateOptions,
  filterUniqueKeys,
  filterUniqueObjects
};
