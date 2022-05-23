function isMultidimensional(array) {
  return Array.isArray(array) && array.some((value) => Array.isArray(value));
}

module.exports = {
  isMultidimensional
};
