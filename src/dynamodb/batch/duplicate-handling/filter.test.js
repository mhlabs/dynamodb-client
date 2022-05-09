const tested = require('./filter');
const testData = require('./filter.test-data.json');

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
      'All keys should must be objects'
    );
  });
  it('should filter out duplicate objects by given root timestamp property', () => {});
  it('should filter out duplicate objects by given nested timestamp property', () => {});
  it('should filter out duplicate objects without timestamp property', () => {});
});
