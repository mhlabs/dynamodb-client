import * as tested from './filter';
import * as testData from './filter.test-data.json';

describe('filter duplicates', () => {
  it('should filter out duplicate keys', () => {
    const keys = testData.map((data) => ({ id: data.id, id2: data.id2 }));
    const uniqueKeys = tested.filterUniqueKeys(keys);

    expect(uniqueKeys.length).toBe(2);
    expect(uniqueKeys[0].id).toBe(1);
    expect(uniqueKeys[1].id).toBe(2);
  });

  it('should verify that keys for filtering are objects', () => {
    const keys = [{}, 'a'];
    expect(() => tested.filterUniqueKeys(keys)).toThrow(
      'All keys must be objects'
    );
  });

  it('should verify that partition key name are set when filtering objects', () => {
    expect(() => tested.filterUniqueObjects(testData)).toThrow(
      'At least partition key attribute must be set in duplicateConfig'
    );
  });

  const options = {
    duplicateConfig: {
      versionAttributeName: 'timestamp',
      partitionKeyAttributeName: 'id',
      sortKeyAttributeName: 'id2'
    }
  };

  it('should filter out duplicate objects by given root timestamp property', () => {
    const uniqueObjects = tested.filterUniqueObjects(testData, options);

    expect(uniqueObjects.length).toBe(2);
    expect(uniqueObjects[0].id).toBe(1);
    expect(uniqueObjects[0].timestamp).toBe('2022-05-09T10:36:12+02:00');
    expect(uniqueObjects[1].id).toBe(2);
  });

  it('should filter out duplicate objects by given nested timestamp property', () => {
    const config = { ...options };
    config.duplicateConfig.versionAttributeName = 'updatedExternal.at';

    const uniqueObjects = tested.filterUniqueObjects(testData, config);

    expect(uniqueObjects.length).toBe(2);
    expect(uniqueObjects[0].id).toBe(1);
    expect(uniqueObjects[0].updatedExternal.at).toBe(
      '2022-05-09T10:36:12+02:00'
    );
    expect(uniqueObjects[1].id).toBe(2);
  });

  it('should filter out duplicate objects without timestamp property', () => {
    const config = { ...options };
    config.duplicateConfig.versionAttributeName = '';

    const uniqueObjects = tested.filterUniqueObjects(testData, config);

    expect(uniqueObjects.length).toBe(2);
    expect(uniqueObjects[0].id).toBe(1);
    expect(uniqueObjects[0].timestamp).toBe('2022-05-02T10:36:12+02:00');
    expect(uniqueObjects[1].id).toBe(2);
  });
});
