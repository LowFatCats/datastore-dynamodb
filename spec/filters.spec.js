/* eslint import/no-extraneous-dependencies: ["error", {"devDependencies": true}] */
require('jasmine-expect');

const filters = require('../lib/filters');

describe('filters', () => {
  describe('parse()', () => {
    it('parses empty lists', () => {
      expect(filters.parse(undefined)).toEqual([]);
      expect(filters.parse(null)).toEqual([]);
      expect(filters.parse('')).toEqual([]);
    });
    it('parses default positive truthy filters', () => {
      expect(filters.parse('key')).toEqual([{ key: 'key', action: 'keep' }]);
    });
    it('parses default positive filters', () => {
      expect(filters.parse('key:value')).toEqual([
        { key: 'key', values: ['value'], action: 'keep' },
      ]);
    });
    it('parses default positive filters with more values', () => {
      expect(filters.parse('key:value1,value2')).toEqual([
        { key: 'key', values: ['value1', 'value2'], action: 'keep' },
      ]);
    });
    it('parses positive truthy filters', () => {
      expect(filters.parse('+key')).toEqual([{ key: 'key', action: 'keep' }]);
    });
    it('parses positive filters', () => {
      expect(filters.parse('+key:value')).toEqual([
        { key: 'key', values: ['value'], action: 'keep' },
      ]);
    });
    it('parses positive filters with more values', () => {
      expect(filters.parse('+key:value1,value2')).toEqual([
        { key: 'key', values: ['value1', 'value2'], action: 'keep' },
      ]);
    });
    it('parses negative truthy filters', () => {
      expect(filters.parse('-key')).toEqual([{ key: 'key', action: 'remove' }]);
    });
    it('parses negative filters', () => {
      expect(filters.parse('-key:value')).toEqual([
        { key: 'key', values: ['value'], action: 'remove' },
      ]);
    });
    it('parses negative filters with more values', () => {
      expect(filters.parse('-key:value1,value2')).toEqual([
        { key: 'key', values: ['value1', 'value2'], action: 'remove' },
      ]);
    });
    it('parses more positive filters', () => {
      expect(filters.parse('key1:value1,value2;key2:value3,value4')).toEqual([
        { key: 'key1', values: ['value1', 'value2'], action: 'keep' },
        { key: 'key2', values: ['value3', 'value4'], action: 'keep' },
      ]);
    });
    it('parses more negative filters', () => {
      expect(filters.parse('-key1:value1,value2;-key2:value3,value4')).toEqual([
        { key: 'key1', values: ['value1', 'value2'], action: 'remove' },
        { key: 'key2', values: ['value3', 'value4'], action: 'remove' },
      ]);
    });
    it('parses positive and negative filters', () => {
      expect(filters.parse('key1:value1,value2;-key2:value3,value4')).toEqual([
        { key: 'key1', values: ['value1', 'value2'], action: 'keep' },
        { key: 'key2', values: ['value3', 'value4'], action: 'remove' },
      ]);
    });
    it('parses same key multiple times', () => {
      expect(filters.parse('key:value1;key:value2;-key:value3')).toEqual([
        { key: 'key', values: ['value1'], action: 'keep' },
        { key: 'key', values: ['value2'], action: 'keep' },
        { key: 'key', values: ['value3'], action: 'remove' },
      ]);
    });
    it('parses truthy and valued filters', () => {
      expect(filters.parse('key;key2:value2')).toEqual([
        { key: 'key', action: 'keep' },
        { key: 'key2', values: ['value2'], action: 'keep' },
      ]);
    });
  });

  describe('apply()', () => {
    it('filters empty lists of data', () => {
      const f = [{ key: 'key', action: 'keep' }];
      expect(filters.apply(undefined, f)).toEqual(undefined);
      expect(filters.apply(null, f)).toEqual(null);
      expect(filters.apply([], f)).toEqual([]);
    });
    it('filters with empty filter', () => {
      expect(filters.apply([{ a: 1 }], null)).toEqual([{ a: 1 }]);
      expect(filters.apply([{ a: 1 }], [])).toEqual([{ a: 1 }]);
    });
    it('filters to keep elements with truthy keys', () => {
      const f = [{ key: 'some', action: 'keep' }];
      expect(
        filters.apply(
          [
            { some: 'a', a: 1 },
            { some: true, a: 2 },
            { some: false, a: 3 },
            { some: '', a: 4 },
            { a: 5 },
          ],
          f
        )
      ).toEqual([
        { some: 'a', a: 1 },
        { some: true, a: 2 },
      ]);
    });
    it('filters to remove elements with truthy keys', () => {
      const f = [{ key: 'some', action: 'remove' }];
      expect(
        filters.apply(
          [
            { some: 'a', a: 1 },
            { some: true, a: 2 },
            { some: false, a: 3 },
            { some: '', a: 4 },
            { a: 5 },
          ],
          f
        )
      ).toEqual([{ some: false, a: 3 }, { some: '', a: 4 }, { a: 5 }]);
    });
    it('filters to keep elements with values', () => {
      const f = [{ key: 'some', values: ['a', 'b'], action: 'keep' }];
      expect(
        filters.apply(
          [
            { some: 'a', a: 1 },
            { some: true, a: 2 },
            { some: 'b', a: 3 },
            { some: '', a: 4 },
            { a: 5 },
          ],
          f
        )
      ).toEqual([
        { some: 'a', a: 1 },
        { some: 'b', a: 3 },
      ]);
    });
    it('filters to remove elements with values', () => {
      const f = [{ key: 'some', values: ['a', 'b'], action: 'remove' }];
      expect(
        filters.apply(
          [
            { some: 'a', a: 1 },
            { some: true, a: 2 },
            { some: 'b', a: 3 },
            { some: '', a: 4 },
            { a: 5 },
          ],
          f
        )
      ).toEqual([{ some: true, a: 2 }, { some: '', a: 4 }, { a: 5 }]);
    });
    it('filters applies multiple filters', () => {
      const f = [
        { key: 'some', values: ['a', 'b'], action: 'keep' },
        { key: 'other', action: 'remove' },
      ];
      expect(
        filters.apply(
          [
            { some: 'a', a: 1, other: '1' },
            { some: true, a: 2 },
            { some: 'b', a: 3 },
            { some: '', a: 4 },
            { a: 5 },
          ],
          f
        )
      ).toEqual([{ some: 'b', a: 3 }]);
    });
    it('filters by casting values to strings', () => {
      const f = [
        { key: 'some', values: ['true'], action: 'keep' },
        { key: 'other', values: ['1'], action: 'keep' },
      ];
      expect(
        filters.apply(
          [
            { some: 'true', a: 1 },
            { some: 'true', a: 2, other: '1' },
            { some: true, a: 3 },
            { some: true, a: 4, other: 1 },
          ],
          f
        )
      ).toEqual([
        { some: 'true', a: 2, other: '1' },
        { some: true, a: 4, other: 1 },
      ]);
    });
    it('filters deep paths with values', () => {
      const f = [{ key: 'some.other', values: ['1'], action: 'keep' }];
      expect(
        filters.apply(
          [
            {
              some: {
                other: 1,
                a: 1,
              },
            },
            {
              some: {
                other: 2,
                a: 2,
              },
            },
          ],
          f
        )
      ).toEqual([
        {
          some: {
            other: 1,
            a: 1,
          },
        },
      ]);
    });
    it('filters deep paths truthy', () => {
      const f = [{ key: 'some.other', action: 'keep' }];
      expect(
        filters.apply(
          [
            {
              some: {
                other: 1,
                a: 1,
              },
            },
            {
              some: {
                a: 2,
              },
            },
          ],
          f
        )
      ).toEqual([
        {
          some: {
            other: 1,
            a: 1,
          },
        },
      ]);
    });
    it('filters array values', () => {
      const f = [
        { key: 'some', values: ['a', 'b'], action: 'keep' },
        { key: 'other', action: 'remove' },
      ];
      expect(
        filters.apply(
          [
            { some: 'a', a: 1, other: '1' },
            { some: true, a: 2 },
            { some: [], a: 3 },
            { some: ['b'], a: 4 },
            { some: ['x', 'b'], a: 5 },
            { some: ['x'], a: 6 },
            { some: 'a', a: 7 },
            { some: '', a: 8 },
            { a: 9 },
          ],
          f
        )
      ).toEqual([
        { some: ['b'], a: 4 },
        { some: ['x', 'b'], a: 5 },
        { some: 'a', a: 7 },
      ]);
    });
    it('filters array values with string conversion', () => {
      const f = [
        { key: 'some', values: ['a', 'true'], action: 'keep' },
        { key: 'other', action: 'remove' },
      ];
      expect(
        filters.apply(
          [
            { some: 'a', a: 1, other: '1' },
            { some: true, a: 2 },
            { some: [], a: 3 },
            { some: ['b'], a: 4 },
            { some: ['x', true], a: 5 },
            { some: ['x'], a: 6 },
            { some: '', a: 7 },
            { a: 8 },
          ],
          f
        )
      ).toEqual([
        { some: true, a: 2 },
        { some: ['x', true], a: 5 },
      ]);
    });
  });
});
