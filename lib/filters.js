const _ = require('lodash');

const filters = {};

class FilterException extends Error {
  constructor(message) {
    super(message);
    this.name = this.constructor.name;
    Error.captureStackTrace(this, this.constructor);
  }
}

filters.FilterException = FilterException;

// Parses a list of field names and values into a filter list.
//
// E.g.
//    animalStatus:Available,Hold;-animalColor:Black;thumb
// =>
// [
//   { key: 'animalStatus', values: ['Available', 'Hold'], action: 'keep' },
//   { key: 'animalColor', values: ['Black'], action: 'remove' },
//   { key: 'thumb', action: 'keep' }
// ]
filters.parse = function (filterString) {
  const result = [];
  const filterItems = _.split(filterString, ';');
  if (filterItems.length) {
    _.forEach(filterItems, element => {
      if (!element.trim()) return;
      const parts = _.split(element, ':', 2);
      if (parts.length > 0) {
        let key = parts[0];
        let values;
        let action = 'keep';
        if (key) {
          if (key[0] === '+') {
            key = key.substring(1);
          } else if (key[0] === '-') {
            key = key.substring(1);
            action = 'remove';
          }
          if (parts.length > 1) {
            values = _.split(parts[1], ',');
          }
          if (values) {
            result.push({ key, values, action });
          } else {
            result.push({ key, action });
          }
        }
      }
    });
  }
  return result;
};

// Applies a set of filters to data to keep or remove elements
// from an array of elements.
filters.apply = function (data, filters) {
  if (!data || !data.length || !filters || !filters.length) return data;
  let filtered = data;
  _.forEach(filters, filter => {
    if (!filter) return;
    let predicate;
    if (_.isArray(filter.values)) {
      predicate = item => {
        const toTest = _.get(item, filter.key);
        if (_.isArray(toTest)) {
          for (const key of toTest) {
            if (filter.values.indexOf(String(key)) > -1) {
              return true;
            }
          }
          return false;
        }
        return filter.values.indexOf(String(toTest)) > -1;
      };
    } else {
      predicate = _.property(filter.key);
    }
    if (filter.action === 'keep') {
      filtered = _.filter(filtered, predicate);
    } else {
      // remove
      filtered = _.filter(filtered, item => !predicate(item));
    }
  });
  return filtered;
};

module.exports = filters;
