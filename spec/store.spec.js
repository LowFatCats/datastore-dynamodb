/* eslint import/no-extraneous-dependencies: ["error", {"devDependencies": true}] */
require('jasmine-expect');

process.env.STORE_PREFIX = 'Test_';
process.env.DYNAMO_DB_ENDPOINT = 'http://localhost:4567';

const dynalite = require('dynalite');
const store = require('../index');
const loadFixtures = require('./helpers/fixtures');
const utils = require('./helpers/utils');
const _ = require('lodash');

describe('store', () => {
  beforeEach(done => {
    console.log = jasmine.createSpy('log');
    this.dynaliteServer = dynalite({ createTableMs: 20 });
    this.dynaliteServer.listen(4567, err => {
      if (err) throw err;
      done();
    });
  });

  afterEach(() => {
    this.dynaliteServer.close();
  });

  describe('with fixtures', () => {
    beforeEach(done => {
      store
        .createTables()
        .then(utils.delay(30))
        .then(() => loadFixtures())
        .then(done)
        .catch(done.fail);
    });

    describe('get()', () => {
      it('retrieves full content by id', done => {
        store
          .get('event#1')
          .then(result => {
            expect(result).toEqual({
              Item: {
                ID: 'event#1',
                Data: {
                  Type: 'event',
                  title: 'Event Name #1',
                  link: '#',
                  date: '2017-10-03T16:00:00.000Z',
                },
                CreatedAt: jasmine.any(String),
                CreatedBy: 'fixture',
              },
              ConsumedCapacity: {
                TableName: 'Test_Content',
                CapacityUnits: jasmine.any(Number),
              },
            });
          })
          .then(done)
          .catch(done.fail);
      });

      it('does not return an Item when there is no content with a specific id', done => {
        store
          .get('unknown-content')
          .then(result => {
            expect(result).not.toHaveMember('Item');
            expect(result).toEqual({
              ConsumedCapacity: {
                TableName: 'Test_Content',
                CapacityUnits: jasmine.any(Number),
              },
            });
          })
          .then(done)
          .catch(done.fail);
      });

      it('throws an exception when trying to get a content using an invalid id type', done => {
        store
          .get(123)
          .then(() => {
            done.fail(new Error('Promise should not be resolved'));
          })
          .catch(error => {
            expect(error.name).toEqual('ValidationException');
          })
          .then(done)
          .catch(done.fail);
      });

      it('allows items that pass positive filter', done => {
        store
          .get('cat#0', { filter: 'age:Senior' })
          .then(result => {
            expect(result).toHaveMember('Item');
            expect(result.Item).toEqual({
              ID: 'cat#0',
              Data: {
                Type: 'cat',
                IID: '0',
                link: 'article-profile.html',
                name: 'Marigold',
                gen: 'f',
                image: 'images/cat-marigold--1__thumb.jpg',
                size: 'Medium',
                age: 'Senior',
                breed: 'Tabby',
                color: 'Red Tabby',
                dob: '03/2004',
                special: 'y',
              },
              CreatedAt: jasmine.any(String),
              CreatedBy: 'fixture',
            });
          })
          .then(done)
          .catch(done.fail);
      });

      it('allows items that pass negative filter', done => {
        store
          .get('cat#0', { filter: '-age:Baby' })
          .then(result => {
            expect(result).toHaveMember('Item');
            expect(result.Item).toEqual({
              ID: 'cat#0',
              Data: {
                Type: 'cat',
                IID: '0',
                link: 'article-profile.html',
                name: 'Marigold',
                gen: 'f',
                image: 'images/cat-marigold--1__thumb.jpg',
                size: 'Medium',
                age: 'Senior',
                breed: 'Tabby',
                color: 'Red Tabby',
                dob: '03/2004',
                special: 'y',
              },
              CreatedAt: jasmine.any(String),
              CreatedBy: 'fixture',
            });
          })
          .then(done)
          .catch(done.fail);
      });

      it('rejects items that do not pass positive filter', done => {
        store
          .get('cat#0', { filter: 'age:Young' })
          .then(() => {
            done.fail(new Error('Promise should not be resolved'));
          })
          .catch(error => {
            expect(error.name).toEqual('FilterException');
          })
          .then(done)
          .catch(done.fail);
      });

      it('rejects items that do not pass negative filter', done => {
        store
          .get('cat#0', { filter: '-age:Senior' })
          .then(() => {
            done.fail(new Error('Promise should not be resolved'));
          })
          .catch(error => {
            expect(error.name).toEqual('FilterException');
          })
          .then(done)
          .catch(done.fail);
      });
    });

    describe('getBrief()', () => {
      it('retrieves brief content by type and iid', done => {
        store
          .getBrief('cat', '2')
          .then(result => {
            expect(result).toEqual({
              Item: {
                Type: 'cat',
                IID: '2',
                FeatureDate: 1502727027330,
                link: 'article-profile.html',
                name: 'Greta',
                gen: 'f',
                image: 'images/cat-greta_thumb.jpg',
                size: 'Medium',
                age: 'Young',
                breed: 'Domestic Long Hair',
                color: 'Black and White',
                dob: '03/05/2013',
              },
              ConsumedCapacity: {
                TableName: 'Test_Brief',
                CapacityUnits: jasmine.any(Number),
              },
            });
          })
          .then(done)
          .catch(done.fail);
      });

      it('does not return a brief Item when there is no content with a specific type and iid combination', done => {
        store
          .getBrief('cat', '42')
          .then(result => {
            expect(result).not.toHaveMember('Item');
            expect(result).toEqual({
              ConsumedCapacity: {
                TableName: 'Test_Brief',
                CapacityUnits: jasmine.any(Number),
              },
            });
          })
          .then(done)
          .catch(done.fail);
      });

      it('throws an exception when trying to get a brief content using an invalid iid type', done => {
        store
          .getBrief('cat', 1)
          .then(() => {
            done.fail(new Error('Promise should not be resolved'));
          })
          .catch(error => {
            expect(error.name).toEqual('ValidationException');
          })
          .then(done)
          .catch(done.fail);
      });
    });

    describe('delete()', () => {
      it('deletes existing full content by id', done => {
        store
          .delete('event#1')
          .then(result => {
            expect(result).toEqual({
              ConsumedCapacity: {
                TableName: 'Test_Content',
                CapacityUnits: jasmine.any(Number),
              },
            });
          })
          .then(done)
          .catch(done.fail);
      });

      it('deletes non-existing full content by id', done => {
        store
          .delete('unknown-content')
          .then(result => {
            expect(result).toEqual({
              ConsumedCapacity: {
                TableName: 'Test_Content',
                CapacityUnits: jasmine.any(Number),
              },
            });
          })
          .then(done)
          .catch(done.fail);
      });

      it('cannot retrieve item after it was deleted', done => {
        store
          .get('event#1')
          .then(result => {
            expect(result).toHaveMember('Item');
          })
          .then(() => {
            return store.delete('event#1');
          })
          .then(() => {
            return store.get('event#1').then(result => {
              expect(result).not.toHaveMember('Item');
            });
          })
          .then(done)
          .catch(done.fail);
      });
    });

    describe('deleteBrief()', () => {
      it('deletes existing brief content', done => {
        store
          .deleteBrief('cat', '2')
          .then(result => {
            expect(result).toEqual({
              ConsumedCapacity: {
                TableName: 'Test_Brief',
                CapacityUnits: jasmine.any(Number),
              },
            });
          })
          .then(done)
          .catch(done.fail);
      });

      it('deletes non-existing brief content', done => {
        store
          .deleteBrief('unknown-type', 'unknown-iid')
          .then(result => {
            expect(result).toEqual({
              ConsumedCapacity: {
                TableName: 'Test_Brief',
                CapacityUnits: jasmine.any(Number),
              },
            });
          })
          .then(done)
          .catch(done.fail);
      });

      it('cannot retrieve item after it was deleted', done => {
        store
          .getBrief('cat', '2')
          .then(result => {
            expect(result).toHaveMember('Item');
          })
          .then(() => {
            return store.deleteBrief('cat', '2');
          })
          .then(() => {
            return store.getBrief('cat', '2').then(result => {
              expect(result).not.toHaveMember('Item');
            });
          })
          .then(done)
          .catch(done.fail);
      });
    });

    describe('getBatchBrief()', () => {
      it('retrieves a batch of brief items', done => {
        store
          .getBatchBrief('cat', ['2', '3'])
          .then(result => {
            expect(result).toEqual({
              Items: [
                {
                  Type: 'cat',
                  IID: '2',
                  FeatureDate: 1502727027330,
                  link: 'article-profile.html',
                  name: 'Greta',
                  gen: 'f',
                  image: 'images/cat-greta_thumb.jpg',
                  size: 'Medium',
                  age: 'Young',
                  breed: 'Domestic Long Hair',
                  color: 'Black and White',
                  dob: '03/05/2013',
                },
                {
                  Type: 'cat',
                  IID: '3',
                  link: 'article-profile.html',
                  name: 'Hagrid',
                  gen: 'm',
                  image: 'images/cat-hagrid_thumb.jpg',
                  size: 'Medium',
                  age: 'Adult',
                  breed: 'Domestic Medium Hair',
                  color: 'Gray and White',
                  dob: '08/09/2014',
                },
              ],
              ConsumedCapacity: [
                {
                  TableName: 'Test_Brief',
                  CapacityUnits: jasmine.any(Number),
                },
              ],
              UnprocessedKeys: {},
            });
          })
          .then(done)
          .catch(done.fail);
      });

      it('retrieves a batch of brief items with one element', done => {
        store
          .getBatchBrief('cat', ['3'])
          .then(result => {
            expect(result).toEqual({
              Items: [
                {
                  Type: 'cat',
                  IID: '3',
                  link: 'article-profile.html',
                  name: 'Hagrid',
                  gen: 'm',
                  image: 'images/cat-hagrid_thumb.jpg',
                  size: 'Medium',
                  age: 'Adult',
                  breed: 'Domestic Medium Hair',
                  color: 'Gray and White',
                  dob: '08/09/2014',
                },
              ],
              ConsumedCapacity: [
                {
                  TableName: 'Test_Brief',
                  CapacityUnits: jasmine.any(Number),
                },
              ],
              UnprocessedKeys: {},
            });
          })
          .then(done)
          .catch(done.fail);
      });

      it('retrieves a batch of brief items with no element', done => {
        store
          .getBatchBrief('cat', [])
          .then(result => {
            expect(result).toEqual({
              Items: [],
            });
          })
          .then(done)
          .catch(done.fail);
      });

      it('retrieves a batch of brief items with duplicate elements', done => {
        store
          .getBatchBrief('cat', ['2', '3', '2'])
          .then(result => {
            expect(result).toEqual({
              Items: [
                {
                  Type: 'cat',
                  IID: '2',
                  FeatureDate: 1502727027330,
                  link: 'article-profile.html',
                  name: 'Greta',
                  gen: 'f',
                  image: 'images/cat-greta_thumb.jpg',
                  size: 'Medium',
                  age: 'Young',
                  breed: 'Domestic Long Hair',
                  color: 'Black and White',
                  dob: '03/05/2013',
                },
                {
                  Type: 'cat',
                  IID: '3',
                  link: 'article-profile.html',
                  name: 'Hagrid',
                  gen: 'm',
                  image: 'images/cat-hagrid_thumb.jpg',
                  size: 'Medium',
                  age: 'Adult',
                  breed: 'Domestic Medium Hair',
                  color: 'Gray and White',
                  dob: '08/09/2014',
                },
                {
                  Type: 'cat',
                  IID: '2',
                  FeatureDate: 1502727027330,
                  link: 'article-profile.html',
                  name: 'Greta',
                  gen: 'f',
                  image: 'images/cat-greta_thumb.jpg',
                  size: 'Medium',
                  age: 'Young',
                  breed: 'Domestic Long Hair',
                  color: 'Black and White',
                  dob: '03/05/2013',
                },
              ],
              ConsumedCapacity: [
                {
                  TableName: 'Test_Brief',
                  CapacityUnits: jasmine.any(Number),
                },
              ],
              UnprocessedKeys: {},
            });
          })
          .then(done)
          .catch(done.fail);
      });

      it('retrieves a batch of brief items that includes unknown element', done => {
        store
          .getBatchBrief('cat', ['first-unknown', '3', 'second-unknown'])
          .then(result => {
            expect(result).toEqual({
              Items: [
                {
                  Type: 'cat',
                  IID: '3',
                  link: 'article-profile.html',
                  name: 'Hagrid',
                  gen: 'm',
                  image: 'images/cat-hagrid_thumb.jpg',
                  size: 'Medium',
                  age: 'Adult',
                  breed: 'Domestic Medium Hair',
                  color: 'Gray and White',
                  dob: '08/09/2014',
                },
              ],
              ConsumedCapacity: [
                {
                  TableName: 'Test_Brief',
                  CapacityUnits: jasmine.any(Number),
                },
              ],
              UnprocessedKeys: {},
            });
          })
          .then(done)
          .catch(done.fail);
      });

      it('retrieves an empty batch of brief items if it includes only unknown elements', done => {
        store
          .getBatchBrief('cat', ['first-unknown', 'second-unknown'])
          .then(result => {
            expect(result).toEqual({
              Items: [],
              ConsumedCapacity: [
                {
                  TableName: 'Test_Brief',
                  CapacityUnits: jasmine.any(Number),
                },
              ],
              UnprocessedKeys: {},
            });
          })
          .then(done)
          .catch(done.fail);
      });

      it('throws an exception when trying to get a batch using invalid id types', done => {
        store
          .getBatchBrief('cat', [2, 3])
          .then(() => {
            done.fail(new Error('Promise should not be resolved'));
          })
          .catch(error => {
            expect(error.name).toEqual('ValidationException');
          })
          .then(done)
          .catch(done.fail);
      });
    });

    describe('getBatch()', () => {
      it('retrieves a batch of full items', done => {
        store
          .getBatch(['cat#2', 'cat#3'])
          .then(result => {
            expect(result).toEqual({
              Items: [
                {
                  ID: 'cat#2',
                  Data: {
                    Type: 'cat',
                    IID: '2',
                    link: 'article-profile.html',
                    name: 'Greta',
                    gen: 'f',
                    image: 'images/cat-greta_thumb.jpg',
                    size: 'Medium',
                    age: 'Young',
                    breed: 'Domestic Long Hair',
                    color: 'Black and White',
                    dob: '03/05/2013',
                  },
                  CreatedAt: jasmine.any(String),
                  CreatedBy: 'fixture',
                },
                {
                  ID: 'cat#3',
                  Data: {
                    Type: 'cat',
                    IID: '3',
                    link: 'article-profile.html',
                    name: 'Hagrid',
                    gen: 'm',
                    image: 'images/cat-hagrid_thumb.jpg',
                    size: 'Medium',
                    age: 'Adult',
                    breed: 'Domestic Medium Hair',
                    color: 'Gray and White',
                    dob: '08/09/2014',
                  },
                  CreatedAt: jasmine.any(String),
                  CreatedBy: 'fixture',
                },
              ],
              ConsumedCapacity: [
                {
                  TableName: 'Test_Content',
                  CapacityUnits: jasmine.any(Number),
                },
              ],
              UnprocessedKeys: {},
            });
          })
          .then(done)
          .catch(done.fail);
      });

      it('retrieves a batch of full items with one element', done => {
        store
          .getBatch(['cat#3'])
          .then(result => {
            expect(result).toEqual({
              Items: [
                {
                  ID: 'cat#3',
                  Data: {
                    Type: 'cat',
                    IID: '3',
                    link: 'article-profile.html',
                    name: 'Hagrid',
                    gen: 'm',
                    image: 'images/cat-hagrid_thumb.jpg',
                    size: 'Medium',
                    age: 'Adult',
                    breed: 'Domestic Medium Hair',
                    color: 'Gray and White',
                    dob: '08/09/2014',
                  },
                  CreatedAt: jasmine.any(String),
                  CreatedBy: 'fixture',
                },
              ],
              ConsumedCapacity: [
                {
                  TableName: 'Test_Content',
                  CapacityUnits: jasmine.any(Number),
                },
              ],
              UnprocessedKeys: {},
            });
          })
          .then(done)
          .catch(done.fail);
      });

      it('retrieves a batch of fulle items with no element', done => {
        store
          .getBatch([])
          .then(result => {
            expect(result).toEqual({
              Items: [],
            });
          })
          .then(done)
          .catch(done.fail);
      });

      it('retrieves a batch of full items with duplicate elements', done => {
        store
          .getBatch(['cat#2', 'cat#3', 'cat#2'])
          .then(result => {
            expect(result).toEqual({
              Items: [
                {
                  ID: 'cat#2',
                  Data: {
                    Type: 'cat',
                    IID: '2',
                    link: 'article-profile.html',
                    name: 'Greta',
                    gen: 'f',
                    image: 'images/cat-greta_thumb.jpg',
                    size: 'Medium',
                    age: 'Young',
                    breed: 'Domestic Long Hair',
                    color: 'Black and White',
                    dob: '03/05/2013',
                  },
                  CreatedAt: jasmine.any(String),
                  CreatedBy: 'fixture',
                },
                {
                  ID: 'cat#3',
                  Data: {
                    Type: 'cat',
                    IID: '3',
                    link: 'article-profile.html',
                    name: 'Hagrid',
                    gen: 'm',
                    image: 'images/cat-hagrid_thumb.jpg',
                    size: 'Medium',
                    age: 'Adult',
                    breed: 'Domestic Medium Hair',
                    color: 'Gray and White',
                    dob: '08/09/2014',
                  },
                  CreatedAt: jasmine.any(String),
                  CreatedBy: 'fixture',
                },
                {
                  ID: 'cat#2',
                  Data: {
                    Type: 'cat',
                    IID: '2',
                    link: 'article-profile.html',
                    name: 'Greta',
                    gen: 'f',
                    image: 'images/cat-greta_thumb.jpg',
                    size: 'Medium',
                    age: 'Young',
                    breed: 'Domestic Long Hair',
                    color: 'Black and White',
                    dob: '03/05/2013',
                  },
                  CreatedAt: jasmine.any(String),
                  CreatedBy: 'fixture',
                },
              ],
              ConsumedCapacity: [
                {
                  TableName: 'Test_Content',
                  CapacityUnits: jasmine.any(Number),
                },
              ],
              UnprocessedKeys: {},
            });
          })
          .then(done)
          .catch(done.fail);
      });

      it('retrieves a batch of full items that includes unknown element', done => {
        store
          .getBatch(['first-unknown', 'cat#3', 'second-unknown'])
          .then(result => {
            expect(result).toEqual({
              Items: [
                {
                  ID: 'cat#3',
                  Data: {
                    Type: 'cat',
                    IID: '3',
                    link: 'article-profile.html',
                    name: 'Hagrid',
                    gen: 'm',
                    image: 'images/cat-hagrid_thumb.jpg',
                    size: 'Medium',
                    age: 'Adult',
                    breed: 'Domestic Medium Hair',
                    color: 'Gray and White',
                    dob: '08/09/2014',
                  },
                  CreatedAt: jasmine.any(String),
                  CreatedBy: 'fixture',
                },
              ],
              ConsumedCapacity: [
                {
                  TableName: 'Test_Content',
                  CapacityUnits: jasmine.any(Number),
                },
              ],
              UnprocessedKeys: {},
            });
          })
          .then(done)
          .catch(done.fail);
      });

      it('retrieves an empty batch of full items if it includes only unknown elements', done => {
        store
          .getBatch(['first-unknown', 'second-unknown'])
          .then(result => {
            expect(result).toEqual({
              Items: [],
              ConsumedCapacity: [
                {
                  TableName: 'Test_Content',
                  CapacityUnits: jasmine.any(Number),
                },
              ],
              UnprocessedKeys: {},
            });
          })
          .then(done)
          .catch(done.fail);
      });

      it('throws an exception when trying to get a full batch using invalid id types', done => {
        store
          .getBatch([2, 3])
          .then(() => {
            done.fail(new Error('Promise should not be resolved'));
          })
          .catch(error => {
            expect(error.name).toEqual('ValidationException');
          })
          .then(done)
          .catch(done.fail);
      });
    });

    describe('queryByTypeTS()', () => {
      it('returns list of items if the type is known and no timestamp filters', done => {
        store
          .queryByTypeTS('dog-update')
          .then(result => {
            expect(result.Items).toBeArrayOfSize(5);
          })
          .then(done)
          .catch(done.fail);
      });

      it('returns empty list of items if the type is unknown', done => {
        store
          .queryByTypeTS('unknown')
          .then(result => {
            expect(result.Items).toBeEmptyArray();
          })
          .then(done)
          .catch(done.fail);
      });

      it('limits the returned results if requested', done => {
        store
          .queryByTypeTS('dog-update', { limit: 3 })
          .then(result => {
            expect(result.Items).toBeArrayOfSize(3);
          })
          .then(done)
          .catch(done.fail);
      });

      it('properly converts integers and booleans', done => {
        store
          .queryByTypeTS('dog-update', { limit: '3', ascending: 'true' })
          .then(result => {
            expect(result.Items).toBeArrayOfSize(3);
          })
          .then(done)
          .catch(done.fail);
      });

      it('uses minTS and maxTS to filter the results ascending by default', done => {
        store
          .queryByTypeTS('dog-update', {
            minTS: 1501863044192,
            maxTS: 1501949438656,
          })
          .then(result => {
            expect(result.Items).toBeArrayOfSize(2);
            expect(result.Items).toEqual([
              {
                Type: 'dog-update',
                IID: 'id2.48',
                TS: 1501863044192,
                link: 'article-profile.html',
                name: 'Macy',
                'image--s': 'images/dog-macy_thumbs.jpg',
                update: 'adopted',
              },
              {
                Type: 'dog-update',
                IID: 'id2.47',
                TS: 1501949438656,
                link: 'article-profile.html',
                name: 'Cooper and Kea',
                'image--s': 'images/dog-cooper-and-kea_thumbs.jpg',
                update: 'adopted',
              },
            ]);
          })
          .then(done)
          .catch(done.fail);
      });

      it('returns results in descending order if requested', done => {
        store
          .queryByTypeTS('dog-update', {
            minTS: 1501863044192,
            maxTS: 1501949438656,
            ascending: false,
          })
          .then(result => {
            expect(result.Items).toBeArrayOfSize(2);
            expect(result.Items).toEqual([
              {
                Type: 'dog-update',
                IID: 'id2.47',
                TS: 1501949438656,
                link: 'article-profile.html',
                name: 'Cooper and Kea',
                'image--s': 'images/dog-cooper-and-kea_thumbs.jpg',
                update: 'adopted',
              },
              {
                Type: 'dog-update',
                IID: 'id2.48',
                TS: 1501863044192,
                link: 'article-profile.html',
                name: 'Macy',
                'image--s': 'images/dog-macy_thumbs.jpg',
                update: 'adopted',
              },
            ]);
          })
          .then(done)
          .catch(done.fail);
      });

      it('returns results in ascending order if requested', done => {
        store
          .queryByTypeTS('dog-update', {
            minTS: 1501863044192,
            maxTS: 1501949438656,
            ascending: true,
          })
          .then(result => {
            expect(result.Items).toBeArrayOfSize(2);
            expect(result.Items).toEqual([
              {
                Type: 'dog-update',
                IID: 'id2.48',
                TS: 1501863044192,
                link: 'article-profile.html',
                name: 'Macy',
                'image--s': 'images/dog-macy_thumbs.jpg',
                update: 'adopted',
              },
              {
                Type: 'dog-update',
                IID: 'id2.47',
                TS: 1501949438656,
                link: 'article-profile.html',
                name: 'Cooper and Kea',
                'image--s': 'images/dog-cooper-and-kea_thumbs.jpg',
                update: 'adopted',
              },
            ]);
          })
          .then(done)
          .catch(done.fail);
      });

      it('returns one result for exact matching TS', done => {
        store
          .queryByTypeTS('dog-update', {
            minTS: 1501863044192,
            maxTS: 1501863044192,
          })
          .then(result => {
            expect(result.Items).toEqual([
              {
                Type: 'dog-update',
                IID: 'id2.48',
                TS: 1501863044192,
                link: 'article-profile.html',
                name: 'Macy',
                'image--s': 'images/dog-macy_thumbs.jpg',
                update: 'adopted',
              },
            ]);
          })
          .then(done)
          .catch(done.fail);
      });

      it('returns no result if TS is too restrictive', done => {
        store
          .queryByTypeTS('dog-update', {
            minTS: 1501863044193,
            maxTS: 1501949438655,
          })
          .then(result => {
            expect(result.Items).toBeEmptyArray();
          })
          .then(done)
          .catch(done.fail);
      });

      it('can limit using minTS only', done => {
        store
          .queryByTypeTS('dog-update', {
            minTS: 1501949438656,
          })
          .then(result => {
            expect(result.Items).toBeArrayOfSize(4);
          })
          .then(done)
          .catch(done.fail);
      });

      it('can limit using maxTS only', done => {
        store
          .queryByTypeTS('dog-update', {
            maxTS: 1501949438656,
          })
          .then(result => {
            expect(result.Items).toBeArrayOfSize(2);
            expect(result.Items.map(item => item.IID)).toEqual(['id2.48', 'id2.47']);
          })
          .then(done)
          .catch(done.fail);
      });

      it('can filter items using startDate and endDate', done => {
        store
          .queryByTypeTS('dog-update', {
            startDate: '2017-08-05',
            endDate: '2017-08-09',
          })
          .then(result => {
            expect(result.Items).toBeArrayOfSize(2);
            expect(result.Items.map(item => item.IID)).toEqual(['id2.47', 'id2.43']);
          })
          .then(done)
          .catch(done.fail);
      });

      it('can filter items in descending order if startDate > endDate', done => {
        store
          .queryByTypeTS('dog-update', {
            startDate: '2017-08-09',
            endDate: '2017-08-05',
          })
          .then(result => {
            expect(result.Items).toBeArrayOfSize(2);
            expect(result.Items.map(item => item.IID)).toEqual(['id2.43', 'id2.47']);
          })
          .then(done)
          .catch(done.fail);
      });

      it('uses minTS and maxTS if TS and dates are specified', done => {
        store
          .queryByTypeTS('dog-update', {
            startDate: '2017-08-09',
            endDate: '2017-08-05',
            minTS: 1501863044192,
            maxTS: 1501949438656,
          })
          .then(result => {
            expect(result.Items).toBeArrayOfSize(2);
            expect(result.Items.map(item => item.IID)).toEqual(['id2.47', 'id2.48']);
          })
          .then(done)
          .catch(done.fail);
      });

      it('filters data using filter option', done => {
        store
          .queryByTypeTS('dog-update', {
            startDate: '2017-08-09',
            endDate: '2017-08-05',
            minTS: 1501863044192,
            maxTS: 1501949438656,
            filter: 'IID:id2.47;-IID:id2.48',
          })
          .then(result => {
            expect(result.Items).toBeArrayOfSize(1);
            expect(result.Items.map(item => item.IID)).toEqual(['id2.47']);
          })
          .then(done)
          .catch(done.fail);
      });

      it('throws an exception when using an incorect data types', done => {
        store
          .queryByTypeTS('dog-update', { minTS: 'number' })
          .then(() => {
            done.fail(new Error('Promise should not be resolved'));
          })
          .catch(error => {
            expect(error.name).toEqual('ValidationException');
          })
          .then(done)
          .catch(done.fail);
      });
    });

    describe('queryByTypeFeatured()', () => {
      it('returns all featured items of a type in descending order by default', done => {
        store
          .queryByTypeFeatured('cat')
          .then(result => {
            expect(result.Items).toBeArrayOfSize(5);
            expect(result.Items.map(item => item.IID)).toEqual(['0', '1', '2', '4', '5']);
          })
          .then(done)
          .catch(done.fail);
      });

      it('can limit the number of results', done => {
        store
          .queryByTypeFeatured('cat', { limit: 3 })
          .then(result => {
            expect(result.Items).toBeArrayOfSize(3);
            expect(result.Items.map(item => item.IID)).toEqual(['0', '1', '2']);
          })
          .then(done)
          .catch(done.fail);
      });

      it('filters results based on filter option', done => {
        store
          .queryByTypeFeatured('cat', { limit: 3, filter: 'IID:0,2;-IID:1' })
          .then(result => {
            expect(result.Items).toBeArrayOfSize(2);
            expect(result.Items.map(item => item.IID)).toEqual(['0', '2']);
          })
          .then(done)
          .catch(done.fail);
      });

      it('returns all featured items of a type in ascending order', done => {
        store
          .queryByTypeFeatured('cat', { ascending: true })
          .then(result => {
            expect(result.Items).toBeArrayOfSize(5);
            expect(result.Items.map(item => item.IID)).toEqual(['5', '4', '2', '1', '0']);
          })
          .then(done)
          .catch(done.fail);
      });

      it('properly converts integers and booleans in queryByTypeFeatured', done => {
        store
          .queryByTypeFeatured('cat', { limit: '4', ascending: 'false' })
          .then(result => {
            expect(result.Items).toBeArrayOfSize(4);
            expect(result.Items.map(item => item.IID)).toEqual(['0', '1', '2', '4']);
          })
          .then(done)
          .catch(done.fail);
      });

      it('returns no items if there are none of a type', done => {
        store
          .queryByTypeFeatured('unknown-type')
          .then(result => {
            expect(result.Items).toBeEmptyArray();
          })
          .then(done)
          .catch(done.fail);
      });

      describe('with mocked time', () => {
        beforeEach(() => {
          jasmine.clock().mockDate(new Date(1502727027330));
        });

        afterEach(() => {
          jasmine.clock().uninstall();
        });

        it('ignores featured items from the future', done => {
          store
            .queryByTypeFeatured('cat')
            .then(result => {
              expect(result.Items).toBeArrayOfSize(3);
              expect(result.Items.map(item => item.IID)).toEqual(['2', '4', '5']);
            })
            .then(done)
            .catch(done.fail);
        });
      });
    });

    describe('getList()', () => {
      it('returns a list of brief items if ids are specified', done => {
        store
          .getList('cat', { ids: '0,1' })
          .then(result => {
            expect(result.Items).toBeArrayOfSize(2);
            expect(result.Items).toEqual([
              {
                Type: 'cat',
                IID: '0',
                FeatureDate: 1502727027332,
                link: 'article-profile.html',
                name: 'Marigold',
                gen: 'f',
                image: 'images/cat-marigold--1__thumb.jpg',
                size: 'Medium',
                age: 'Senior',
                breed: 'Tabby',
                color: 'Red Tabby',
                dob: '03/2004',
                special: 'y',
              },
              {
                Type: 'cat',
                IID: '1',
                FeatureDate: 1502727027331,
                link: 'article-profile.html',
                name: 'Delilah',
                gen: 'f',
                image: 'images/cat-delilah_thumb.jpg',
                size: 'Medium',
                age: 'Adult',
                breed: 'Domestic Short Hair',
                color: 'Brown',
                dob: '05/2012',
              },
            ]);
          })
          .then(done)
          .catch(done.fail);
      });

      it('returns a list of brief items if ids are specified with type# prefix', done => {
        store
          .getList('cat', { ids: 'cat#0,cat#1' })
          .then(result => {
            expect(result.Items).toBeArrayOfSize(2);
            expect(result.Items).toEqual([
              {
                Type: 'cat',
                IID: '0',
                FeatureDate: 1502727027332,
                link: 'article-profile.html',
                name: 'Marigold',
                gen: 'f',
                image: 'images/cat-marigold--1__thumb.jpg',
                size: 'Medium',
                age: 'Senior',
                breed: 'Tabby',
                color: 'Red Tabby',
                dob: '03/2004',
                special: 'y',
              },
              {
                Type: 'cat',
                IID: '1',
                FeatureDate: 1502727027331,
                link: 'article-profile.html',
                name: 'Delilah',
                gen: 'f',
                image: 'images/cat-delilah_thumb.jpg',
                size: 'Medium',
                age: 'Adult',
                breed: 'Domestic Short Hair',
                color: 'Brown',
                dob: '05/2012',
              },
            ]);
          })
          .then(done)
          .catch(done.fail);
      });

      it('filters brief data if filter is specified', done => {
        store
          .getList('cat', { ids: '0,1', filter: 'age:Adult' })
          .then(result => {
            expect(result.Items).toBeArrayOfSize(1);
            expect(result.Items).toEqual([
              {
                Type: 'cat',
                IID: '1',
                FeatureDate: 1502727027331,
                link: 'article-profile.html',
                name: 'Delilah',
                gen: 'f',
                image: 'images/cat-delilah_thumb.jpg',
                size: 'Medium',
                age: 'Adult',
                breed: 'Domestic Short Hair',
                color: 'Brown',
                dob: '05/2012',
              },
            ]);
          })
          .then(done)
          .catch(done.fail);
      });

      it('ensures int and boolean values are properly converted', done => {
        store
          .getList('cat', { ids: '0,1', limit: '2', full: 'false' })
          .then(result => {
            expect(result.Items).toBeArrayOfSize(2);
            expect(result.Items).toEqual([
              {
                Type: 'cat',
                IID: '0',
                FeatureDate: 1502727027332,
                link: 'article-profile.html',
                name: 'Marigold',
                gen: 'f',
                image: 'images/cat-marigold--1__thumb.jpg',
                size: 'Medium',
                age: 'Senior',
                breed: 'Tabby',
                color: 'Red Tabby',
                dob: '03/2004',
                special: 'y',
              },
              {
                Type: 'cat',
                IID: '1',
                FeatureDate: 1502727027331,
                link: 'article-profile.html',
                name: 'Delilah',
                gen: 'f',
                image: 'images/cat-delilah_thumb.jpg',
                size: 'Medium',
                age: 'Adult',
                breed: 'Domestic Short Hair',
                color: 'Brown',
                dob: '05/2012',
              },
            ]);
          })
          .then(done)
          .catch(done.fail);
      });

      it('returns a list of full items if full flag is present', done => {
        store
          .getList('cat', { ids: 'cat#1,cat#0', full: true })
          .then(result => {
            expect(result.Items).toBeArrayOfSize(2);
            expect(result.Items).toEqual([
              {
                ID: 'cat#1',
                Data: {
                  Type: 'cat',
                  IID: '1',
                  link: 'article-profile.html',
                  name: 'Delilah',
                  gen: 'f',
                  image: 'images/cat-delilah_thumb.jpg',
                  size: 'Medium',
                  age: 'Adult',
                  breed: 'Domestic Short Hair',
                  color: 'Brown',
                  dob: '05/2012',
                },
                CreatedAt: jasmine.any(String),
                CreatedBy: 'fixture',
              },
              {
                ID: 'cat#0',
                Data: {
                  Type: 'cat',
                  IID: '0',
                  link: 'article-profile.html',
                  name: 'Marigold',
                  gen: 'f',
                  image: 'images/cat-marigold--1__thumb.jpg',
                  size: 'Medium',
                  age: 'Senior',
                  breed: 'Tabby',
                  color: 'Red Tabby',
                  dob: '03/2004',
                  special: 'y',
                },
                CreatedAt: jasmine.any(String),
                CreatedBy: 'fixture',
              },
            ]);
          })
          .then(done)
          .catch(done.fail);
      });

      it('filter full items if filter is present', done => {
        store
          .getList('cat', { ids: 'cat#1,cat#0', full: true, filter: 'age:Adult' })
          .then(result => {
            expect(result.Items).toBeArrayOfSize(1);
            expect(result.Items).toEqual([
              {
                ID: 'cat#1',
                Data: {
                  Type: 'cat',
                  IID: '1',
                  link: 'article-profile.html',
                  name: 'Delilah',
                  gen: 'f',
                  image: 'images/cat-delilah_thumb.jpg',
                  size: 'Medium',
                  age: 'Adult',
                  breed: 'Domestic Short Hair',
                  color: 'Brown',
                  dob: '05/2012',
                },
                CreatedAt: jasmine.any(String),
                CreatedBy: 'fixture',
              },
            ]);
          })
          .then(done)
          .catch(done.fail);
      });

      it('returns a list of brief items if more ids are specified', done => {
        store
          .getList('cat', { ids: '0, 1, 2, 3, 4, 5', limit: 2 })
          .then(result => {
            expect(result.Items).toBeArrayOfSize(2);
            expect(result.Items.map(item => item.IID)).toBeArrayOfStrings();
            expect(result.Items.map(item => item.IID)).toEqual(['0', '1']);
          })
          .then(done)
          .catch(done.fail);
      });

      it('returns a slice of brief items for start and limit', done => {
        store
          .getList('cat', { ids: '0, 1, 2, 3, 4, 5', start: 2, limit: 2 })
          .then(result => {
            expect(result.Items).toBeArrayOfSize(2);
            expect(result.Items.map(item => item.IID)).toBeArrayOfStrings();
            expect(result.Items.map(item => item.IID)).toEqual(['2', '3']);
          })
          .then(done)
          .catch(done.fail);
      });

      it('returns brief items from a list-type doc', done => {
        store
          .getList('dog', { list: 'pets--available' })
          .then(result => {
            expect(result.Items).toBeArrayOfSize(5);
            expect(result.Items.map(item => item.IID)).toBeArrayOfStrings();
          })
          .then(done)
          .catch(done.fail);
      });

      it('throws an exception if list and ids options are missing', done => {
        store
          .getList('dog')
          .then(() => {
            done.fail(new Error('Promise should not be resolved'));
          })
          .catch(error => {
            expect(error.message).toEqual('You must specify `list` or `ids` args');
          })
          .then(done)
          .catch(done.fail);
      });
    });

    describe('getRandomList()', () => {
      it('returns a random list of brief items if ids are specified', done => {
        store
          .getRandomList('cat', { ids: '0,1' })
          .then(result => {
            expect(result.Items).toBeArrayOfSize(2);
            expect(_.sortBy(result.Items, 'IID')).toEqual([
              {
                Type: 'cat',
                IID: '0',
                FeatureDate: 1502727027332,
                link: 'article-profile.html',
                name: 'Marigold',
                gen: 'f',
                image: 'images/cat-marigold--1__thumb.jpg',
                size: 'Medium',
                age: 'Senior',
                breed: 'Tabby',
                color: 'Red Tabby',
                dob: '03/2004',
                special: 'y',
              },
              {
                Type: 'cat',
                IID: '1',
                FeatureDate: 1502727027331,
                link: 'article-profile.html',
                name: 'Delilah',
                gen: 'f',
                image: 'images/cat-delilah_thumb.jpg',
                size: 'Medium',
                age: 'Adult',
                breed: 'Domestic Short Hair',
                color: 'Brown',
                dob: '05/2012',
              },
            ]);
          })
          .then(done)
          .catch(done.fail);
      });

      it('returns a random list of brief items if more ids are specified', done => {
        store
          .getRandomList('cat', { ids: '0, 1, 0, 1, 0, 1, 0, 1', limit: 2 })
          .then(result => {
            expect(result.Items).toBeArrayOfSize(2);
            expect(result.Items.map(item => item.IID)).toBeArrayOfStrings();
          })
          .then(done)
          .catch(done.fail);
      });

      it('returns random brief items from a list-type doc', done => {
        store
          .getRandomList('dog', { list: 'pets--available' })
          .then(result => {
            expect(result.Items).toBeArrayOfSize(5);
            expect(result.Items.map(item => item.IID)).toBeArrayOfStrings();
          })
          .then(done)
          .catch(done.fail);
      });

      it('throws an exception if list and ids options are missing in getRandomList', done => {
        store
          .getRandomList('dog')
          .then(() => {
            done.fail(new Error('Promise should not be resolved'));
          })
          .catch(error => {
            expect(error.message).toEqual('You must specify `list` or `ids` args');
          })
          .then(done)
          .catch(done.fail);
      });
    });

    describe('create()', () => {
      it('throws an exception if the item already exists', done => {
        store
          .create('event#1', { some: 'data' }, 'some-author')
          .then(() => {
            done.fail(new Error('Promise should not be resolved'));
          })
          .catch(error => {
            expect(error.name).toEqual('ConditionalCheckFailedException');
          })
          .then(done)
          .catch(done.fail);
      });
    });

    describe('update()', () => {
      it('updates an item if exists', done => {
        store
          .update('event#1', { some: 'data' }, 'some-author')
          .then(result => {
            expect(result).toEqual({
              ConsumedCapacity: {
                TableName: 'Test_Content',
                CapacityUnits: jasmine.any(Number),
              },
            });
          })
          .then(done)
          .catch(done.fail);
      });

      it('can correctly retrieve the updated item', done => {
        store
          .update('event#1', { some: 'data' }, 'some-author')
          .then(() =>
            store.get('event#1').then(result => {
              expect(result).toEqual({
                Item: {
                  ID: 'event#1',
                  Data: {
                    some: 'data',
                  },
                  CreatedAt: jasmine.any(String),
                  CreatedBy: 'fixture',
                  UpdatedAt: jasmine.any(String),
                  UpdatedBy: 'some-author',
                },
                ConsumedCapacity: {
                  TableName: 'Test_Content',
                  CapacityUnits: jasmine.any(Number),
                },
              });
            })
          )
          .then(done)
          .catch(done.fail);
      });
    });

    describe('put()', () => {
      it('updates the item if it already exists', done => {
        store
          .put('event#1', { some: 'data' }, 'some-author')
          .then(result => {
            expect(result.status).toEqual('updated');
          })
          .then(done)
          .catch(done.fail);
      });

      it('can correctly retrieve the item updated using put', done => {
        store
          .put('event#1', { some: 'data' }, 'some-author')
          .then(() =>
            store.get('event#1').then(result => {
              expect(result).toEqual({
                Item: {
                  ID: 'event#1',
                  Data: {
                    some: 'data',
                  },
                  CreatedAt: jasmine.any(String),
                  CreatedBy: 'fixture',
                  UpdatedAt: jasmine.any(String),
                  UpdatedBy: 'some-author',
                },
                ConsumedCapacity: {
                  TableName: 'Test_Content',
                  CapacityUnits: jasmine.any(Number),
                },
              });
            })
          )
          .then(done)
          .catch(done.fail);
      });

      it('throws an exception when trying to put a content using an invalid id type', done => {
        store
          .put(123, { some: 'data' }, 'some-author')
          .then(() => {
            done.fail(new Error('Promise should not be resolved'));
          })
          .catch(error => {
            expect(error.name).toEqual('ValidationException');
          })
          .then(done)
          .catch(done.fail);
      });
    });

    describe('putBrief()', () => {
      it('updates a brief item if it already exists', done => {
        store.putBrief({ Type: 'cat', IID: '2', name: 'some-name' }).then(done).catch(done.fail);
      });

      it('can correctly retrieve the brief item updated', done => {
        store
          .putBrief({ Type: 'cat', IID: '2', name: 'some-name' })
          .then(() =>
            store.getBrief('cat', '2').then(result => {
              expect(result).toEqual({
                Item: {
                  Type: 'cat',
                  IID: '2',
                  name: 'some-name',
                },
                ConsumedCapacity: {
                  TableName: 'Test_Brief',
                  CapacityUnits: jasmine.any(Number),
                },
              });
            })
          )
          .then(done)
          .catch(done.fail);
      });
    });

    describe('scan()', () => {
      it('can correctly scan all content items using non-zera throttle', async () => {
        const items = [];
        const start = Date.now();
        for await (const item of store.scan({ throttle: 10 })) {
          items.push(item);
        }
        const end = Date.now();
        expect(items.length).toEqual(26);
        expect(start + 10 * 25).toBeLessThanOrEqualTo(end);
        expect(start + 15 * 26).toBeGreaterThan(end);
      });

      it('can correctly scan all content items with no throttle', async () => {
        const items = [];
        const start = Date.now();
        for await (const item of store.scan({ throttle: 0 })) {
          items.push(item);
        }
        const end = Date.now();
        expect(items.length).toEqual(26);
        expect(start + 10 * 26).toBeGreaterThan(end);
      });

      it('can correctly scan all content items with no throttle and custom page size', async () => {
        const items = [];
        for await (const item of store.scan({ pageSize: 10, throttle: 0 })) {
          items.push(item);
        }
        expect(items.length).toEqual(26);
      });

      it('retrieves the first item', async () => {
        const item = await store.scan({ throttle: 0 }).next();
        expect(item.done).toBeFalse();
      });
    });

    describe('scanBrief()', () => {
      it('can correctly scan all brief items using the default values', async () => {
        const items = [];
        const start = Date.now();
        for await (const item of store.scanBrief()) {
          items.push(item);
        }
        const end = Date.now();
        expect(items.length).toEqual(35);
        expect(start + 1000).toBeLessThanOrEqualTo(end);
      });

      it('can correctly scan all brief items with no throttle', async () => {
        const items = [];
        const start = Date.now();
        for await (const item of store.scanBrief({ throttle: 0 })) {
          items.push(item);
        }
        const end = Date.now();
        expect(items.length).toEqual(35);
        expect(start + 1000).toBeGreaterThan(end);
      });

      it('can correctly scan all brief items with no throttle and custom page size', async () => {
        const items = [];
        for await (const item of store.scanBrief({ pageSize: 1, throttle: 0 })) {
          items.push(item);
        }
        expect(items.length).toEqual(35);
      });

      it('retrieves the first item', async () => {
        const item = await store.scanBrief({ throttle: 0 }).next();
        expect(item.done).toBeFalse();
      });
    });

    describe('queryByType()', () => {
      it('can correctly queries specific type items using the default options', async () => {
        const items = [];
        for await (const item of store.queryByType('dog-update')) {
          items.push(item);
        }
        expect(items.length).toEqual(5);
      });

      it('can correctly queries specific type items using throttle', async () => {
        const items = [];
        const start = Date.now();
        for await (const item of store.queryByType('dog-update', { pageSize: 1, throttle: 100 })) {
          items.push(item);
        }
        const end = Date.now();
        expect(items.length).toEqual(5);
        expect(start + 100).toBeLessThanOrEqualTo(end);
      });

      it('can correctly queries specific type items with no throttle', async () => {
        const items = [];
        const start = Date.now();
        for await (const item of store.queryByType('dog-update', { throttle: 0 })) {
          items.push(item);
        }
        const end = Date.now();
        expect(items.length).toEqual(5);
        expect(start + 1000).toBeGreaterThan(end);
      });

      it('can correctly queries specific type items with no throttle and custom page size', async () => {
        const items = [];
        for await (const item of store.queryByType('dog-update', { pageSize: 1, throttle: 0 })) {
          items.push(item);
        }
        expect(items.length).toEqual(5);
      });

      it('retrieves the first item', async () => {
        const item = await store.queryByType('dog-update', { throttle: 0 }).next();
        expect(item.done).toBeFalse();
      });

      it('can return queries for prefix', async () => {
        const items = [];
        for await (const item of store.queryByType('dog-update', { prefix: 'id1.' })) {
          items.push(item);
        }
        expect(items.length).toEqual(2);
      });

      it('returns results in ascending order by default', async () => {
        const items = [];
        for await (const item of store.queryByType('dog-update', { prefix: 'id1.' })) {
          items.push(item);
        }
        expect(items.length).toEqual(2);
        expect(items[0].IID).toEqual('id1.45');
        expect(items[1].IID).toEqual('id1.46');
      });

      it('returns results in descending order if specified', async () => {
        const items = [];
        const params = { prefix: 'id1.', ascending: false };
        for await (const item of store.queryByType('dog-update', params)) {
          items.push(item);
        }
        expect(items.length).toEqual(2);
        expect(items[0].IID).toEqual('id1.46');
        expect(items[1].IID).toEqual('id1.45');
      });
    });
  });

  describe('without fixtures', () => {
    beforeEach(done => {
      store.createTables().then(utils.delay(30)).then(done).catch(done.fail);
    });

    describe('create()', () => {
      it('creates an item if it does not exist', done => {
        store
          .create('some-id', { some: 'data' }, 'some-author')
          .then(result => {
            expect(result).toEqual({
              ConsumedCapacity: {
                TableName: 'Test_Content',
                CapacityUnits: jasmine.any(Number),
              },
            });
          })
          .then(done)
          .catch(done.fail);
      });

      it('can correctly retrieve the created item', done => {
        store
          .create('some-id', { some: 'data' }, 'some-author')
          .then(() =>
            store.get('some-id').then(result => {
              expect(result).toEqual({
                Item: {
                  ID: 'some-id',
                  Data: {
                    some: 'data',
                  },
                  CreatedAt: jasmine.any(String),
                  CreatedBy: 'some-author',
                  UpdatedAt: jasmine.any(String),
                  UpdatedBy: 'some-author',
                },
                ConsumedCapacity: {
                  TableName: 'Test_Content',
                  CapacityUnits: jasmine.any(Number),
                },
              });
            })
          )
          .then(done)
          .catch(done.fail);
      });
    });

    describe('update()', () => {
      it('throws an exception if the item does not exist', done => {
        store
          .update('some-new-id', { some: 'data' }, 'some-author')
          .then(() => {
            done.fail(new Error('Promise should not be resolved'));
          })
          .catch(error => {
            expect(error.name).toEqual('ConditionalCheckFailedException');
          })
          .then(done)
          .catch(done.fail);
      });
    });

    describe('put()', () => {
      it('creates the item if it does not exist', done => {
        store
          .put('some-id', { some: 'data' }, 'some-author')
          .then(result => {
            expect(result.status).toEqual('created');
          })
          .then(done)
          .catch(done.fail);
      });

      it('can correctly retrieve the item created using put', done => {
        store
          .put('some-id', { some: 'data' }, 'some-author')
          .then(() =>
            store.get('some-id').then(result => {
              expect(result).toEqual({
                Item: {
                  ID: 'some-id',
                  Data: {
                    some: 'data',
                  },
                  CreatedAt: jasmine.any(String),
                  CreatedBy: 'some-author',
                  UpdatedAt: jasmine.any(String),
                  UpdatedBy: 'some-author',
                },
                ConsumedCapacity: {
                  TableName: 'Test_Content',
                  CapacityUnits: jasmine.any(Number),
                },
              });
            })
          )
          .then(done)
          .catch(done.fail);
      });

      it('replaces empty string fields with null', done => {
        store
          .put('some-id', { some: 'data', empty: '', deep: { one: 1, two: '' } }, 'some-author')
          .then(() =>
            store.get('some-id').then(result => {
              expect(result).toEqual({
                Item: {
                  ID: 'some-id',
                  Data: {
                    some: 'data',
                    empty: null,
                    deep: {
                      one: 1,
                      two: null,
                    },
                  },
                  CreatedAt: jasmine.any(String),
                  CreatedBy: 'some-author',
                  UpdatedAt: jasmine.any(String),
                  UpdatedBy: 'some-author',
                },
                ConsumedCapacity: {
                  TableName: 'Test_Content',
                  CapacityUnits: jasmine.any(Number),
                },
              });
            })
          )
          .then(done)
          .catch(done.fail);
      });
    });

    describe('putBrief()', () => {
      it('creates a brief item if it does not exist', done => {
        store.putBrief({ Type: 'car', IID: '42', name: 'some-name' }).then(done).catch(done.fail);
      });

      it('can correctly retrieve the new brief item created', done => {
        store
          .putBrief({ Type: 'car', IID: '42', name: 'some-name' })
          .then(() =>
            store.getBrief('car', '42').then(result => {
              expect(result).toEqual({
                Item: {
                  Type: 'car',
                  IID: '42',
                  name: 'some-name',
                },
                ConsumedCapacity: {
                  TableName: 'Test_Brief',
                  CapacityUnits: jasmine.any(Number),
                },
              });
            })
          )
          .then(done)
          .catch(done.fail);
      });
    });

    describe('scan()', () => {
      it('returns no items if none are present in the db', async () => {
        const items = [];
        for await (const item of store.scanBrief()) {
          items.push(item);
        }
        expect(items.length).toEqual(0);
      });
    });

    describe('scanBrief()', () => {
      it('returns no items if none are present in the db', async () => {
        const items = [];
        for await (const item of store.scanBrief()) {
          items.push(item);
        }
        expect(items.length).toEqual(0);
      });
    });

    describe('queryByType()', () => {
      it('returns no items if none are present in the db', async () => {
        const items = [];
        for await (const item of store.queryByType('some-type')) {
          items.push(item);
        }
        expect(items.length).toEqual(0);
      });
    });
  });
});
