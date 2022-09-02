export interface DuplicateOptions {
  duplicateConfig: {
    partitionKeyAttributeName: string;
    sortKeyAttributeName: string;
    versionAttributeName: string;
  };
}

export const defaultDuplicateOptions = {
  duplicateConfig: {
    partitionKeyAttributeName: '',
    sortKeyAttributeName: '',
    versionAttributeName: ''
  }
};

export const filterUniqueKeys = (
  keyObjects: Record<string, any>[] = []
): Record<string, any>[] => {
  if (!keyObjects || !keyObjects.length) return [];
  if (keyObjects.some((key) => typeof key !== 'object'))
    throw new Error('All keys must be objects.');

  const stringifiedKeyObjects: string[] = [];
  const uniqueKeyObjects: Record<string, any>[] = [];

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
};

export const filterUniqueObjects = (
  objects: Record<string, any>[] = [],
  options: DuplicateOptions = defaultDuplicateOptions
) => {
  if (!options.duplicateConfig.partitionKeyAttributeName) {
    throw new Error(
      'At least partition key attribute must be set in duplicateConfig'
    );
  }

  if (!objects.length) return objects;

  const uniqueObjects: Record<string, any>[] = [];

  objects.forEach((object) => {
    const uniqueKeyAlreadyAdded = uniqueObjects.some((unique) =>
      sameKey(object, unique, options)
    );

    if (uniqueKeyAlreadyAdded) return;

    const latest = getLatestInstance(objects, object, options);
    uniqueObjects.push(latest);
  });

  return uniqueObjects;
};

const sameKey = (
  object1: Record<string, string | number>,
  object2: Record<string, string | number>,
  options: DuplicateOptions
): boolean => {
  const pk = options.duplicateConfig.partitionKeyAttributeName;
  const sk = options.duplicateConfig.sortKeyAttributeName;

  if (sk) return object1[pk] === object2[pk] && object1[sk] === object2[sk];
  return object1[pk] === object2[pk];
};

const getSortAttributeValues = (
  a: Record<string, any>,
  b: Record<string, any>,
  sortAttribute: string
): { a: any; b: any } => {
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
};

const getLatestInstance = (
  objects: Record<string, any>[],
  currentObject: Record<string, any>,
  options: DuplicateOptions
): Record<string, any> => {
  const sortAttribute = options.duplicateConfig.versionAttributeName;

  const instances = objects.filter((object) =>
    sameKey(currentObject, object, options)
  );

  if (instances.length === 1) return instances[0];
  if (!sortAttribute) return instances[instances.length - 1];

  const latest = instances.sort((a, b) => {
    const values = getSortAttributeValues(a, b, sortAttribute);
    return values.b >= values.a ? 1 : -1;
  });

  return latest[0];
};
