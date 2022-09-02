export const isMultidimensional = (array: any[]) => {
  return Array.isArray(array) && array.some((value) => Array.isArray(value));
};
