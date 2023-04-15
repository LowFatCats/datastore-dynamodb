const AWS = require('aws-sdk');
const _ = require('lodash');
const pickRandom = require('pick-random');
const utils = require('@lowfatcats/moment-utils');
const filters = require('./filters');

const { promisify } = require('util');
const sleep = promisify(setTimeout);

const store = {};

const DYNAMO_DB_ENDPOINT = process.env.DYNAMO_DB_ENDPOINT || null;
const STORE_REGION = process.env.STORE_REGION || 'us-east-1';
const STORE_PREFIX = process.env.STORE_PREFIX || 'Dev_';

AWS.config.update({
  region: STORE_REGION,
});

const dynamodb = new AWS.DynamoDB({ endpoint: DYNAMO_DB_ENDPOINT });
const docClient = new AWS.DynamoDB.DocumentClient({ service: dynamodb, convertEmptyValues: true });

console.log('Configured DynamoDB STORE');
console.log(`  endpoint: ${DYNAMO_DB_ENDPOINT}`);
console.log(`  region  : ${STORE_REGION}`);
console.log(`  prefix  : ${STORE_PREFIX}`);

// Creates store tables
store.createTables = function createTables() {
  const tables = [
    // Content
    {
      TableName: `${STORE_PREFIX}Content`,
      KeySchema: [{ AttributeName: 'ID', KeyType: 'HASH' }],
      AttributeDefinitions: [{ AttributeName: 'ID', AttributeType: 'S' }],
      ProvisionedThroughput: {
        ReadCapacityUnits: 10,
        WriteCapacityUnits: 10,
      },
    },
    // Brief Content
    {
      TableName: `${STORE_PREFIX}Brief`,
      KeySchema: [
        { AttributeName: 'Type', KeyType: 'HASH' },
        { AttributeName: 'IID', KeyType: 'RANGE' },
      ],
      AttributeDefinitions: [
        { AttributeName: 'Type', AttributeType: 'S' },
        { AttributeName: 'IID', AttributeType: 'S' },
        { AttributeName: 'TS', AttributeType: 'N' },
        { AttributeName: 'FeatureDate', AttributeType: 'N' },
      ],
      ProvisionedThroughput: {
        ReadCapacityUnits: 15,
        WriteCapacityUnits: 15,
      },
      LocalSecondaryIndexes: [
        // TypeTS Index
        {
          IndexName: 'TypeTS',
          KeySchema: [
            { AttributeName: 'Type', KeyType: 'HASH' },
            { AttributeName: 'TS', KeyType: 'RANGE' },
          ],
          Projection: {
            ProjectionType: 'ALL',
          },
        },
        // TypeFeatured Index
        {
          IndexName: 'TypeFeatured',
          KeySchema: [
            { AttributeName: 'Type', KeyType: 'HASH' },
            { AttributeName: 'FeatureDate', KeyType: 'RANGE' },
          ],
          Projection: {
            ProjectionType: 'ALL',
          },
        },
      ],
    },
  ];

  const promises = [];
  tables.forEach(params => {
    console.log(`Create table: ${params.TableName}`);
    const promise = dynamodb
      .createTable(params)
      .promise()
      .then(() => {
        console.log(`Table ${params.TableName} created`);
      })
      .catch(error => {
        console.error(`Unable to create table ${params.TableName}: ${error}`);
      });
    promises.push(promise);
  });

  return Promise.all(promises);
};

// Creates a full content item in the database.
// Returns a promise.
//
// If an item with the specified id does not exist then the
// following item will be created.
//
// Item = {
//   ID: id,
//   Data: data,
//   CreatedAt: (now; e.g. 2018-02-19T02:35:05.620Z),
//   CreatedBy: author,
//   UpdatedAt: (now; e.g. 2018-02-19T02:35:05.620Z),
//   UpdatedBy: author
// }
//
// If an item with the specified id already exists, then the
// operation fails with ConditionalCheckFailedException.
store.create = function create(id, data, author) {
  return new Promise(resolve => {
    const now = new Date().toISOString();
    const params = {
      TableName: `${STORE_PREFIX}Content`,
      Item: {
        ID: id,
        Data: data,
        CreatedAt: now,
        CreatedBy: author,
        UpdatedAt: now,
        UpdatedBy: author,
      },
      ConditionExpression: 'attribute_not_exists(ID)',
      ReturnConsumedCapacity: 'TOTAL',
    };
    console.log(`${params.TableName}.CREATE: ${id}`);
    resolve(docClient.put(params).promise());
  });
};

// Updates a full content item in the database.
// Returns a promise.
//
// If the item with the specified id exists, then the
// following fields will be updated:
//
//   Data: data,
//   UpdatedAt: (now; e.g. 2018-02-19T02:35:05.620Z),
//   UpdatedBy: author
//
// If an item with the specified id does not exist, then the
// operation fails with ConditionalCheckFailedException.
store.update = function update(id, data, author) {
  return new Promise(resolve => {
    const now = new Date().toISOString();
    const params = {
      TableName: `${STORE_PREFIX}Content`,
      Key: {
        ID: id,
      },
      UpdateExpression: 'SET #d = :d, UpdatedAt = :t, UpdatedBy = :a',
      ConditionExpression: 'attribute_exists(ID)',
      ExpressionAttributeNames: {
        '#d': 'Data',
      },
      ExpressionAttributeValues: {
        ':d': data,
        ':t': now,
        ':a': author,
      },
      ReturnConsumedCapacity: 'TOTAL',
    };
    console.log(`${params.TableName}.UPDATE: ${id}`);
    resolve(docClient.update(params).promise());
  });
};

// Creates or updates a full content item in the database
// Returns a promise.
//
// If the operation succeeds, then a status field will be returned
// with the possible values being 'created' or 'updated'.
store.put = function put(id, data, author) {
  return new Promise(resolve => {
    const updatePromise = store
      .update(id, data, author)
      .then(result => ({ status: 'updated', result }))
      .catch(error => {
        if (error.name === 'ConditionalCheckFailedException') {
          return store.create(id, data, author).then(result => ({ status: 'created', result }));
        }
        throw error;
      });
    resolve(updatePromise);
  });
};

// Creates or updates a brief content item in the database.
// Returns a promise.
//
// The item must have non-empty Type and IID fields.
store.putBrief = function create(item) {
  return new Promise(resolve => {
    const params = {
      TableName: `${STORE_PREFIX}Brief`,
      Item: item,
      ReturnConsumedCapacity: 'TOTAL',
    };
    console.log(`${params.TableName}.PUTBRIEF: ${item.Type} ${item.IID}`);
    resolve(docClient.put(params).promise());
  });
};

// TODO create an idempotent counter using a READ + Conditional Write

// Get full content by id
// Returns a promise
//
// Options accepted:
// - filter: string; additional filters to be applied;
//                   e.g. filter=animalStatus:Available,Hold;-animalColor:Black
//           default undefined
store.get = function get(id, options) {
  return new Promise((resolve, reject) => {
    const opts = Object.assign({}, options);
    const filter = filters.parse(opts.filter);
    const params = {
      TableName: `${STORE_PREFIX}Content`,
      Key: {
        ID: id,
      },
      ReturnConsumedCapacity: 'TOTAL',
    };
    console.log(`${params.TableName}.GET: ${id}`);
    docClient
      .get(params)
      .promise()
      .then(result => {
        if (filter && filter.length && result && result.Item && result.Item.Data) {
          const rejected = filters.apply([result.Item.Data], filter).length === 0;
          if (rejected) {
            return reject(
              new filters.FilterException(`${id} was rejected by filter ${opts.filter}`)
            );
          }
        }
        resolve(result);
      })
      .catch(error => {
        reject(error);
      });
  });
};

// Gets brief content by type and iid
// Returns a promise
store.getBrief = function get(type, iid) {
  return new Promise(resolve => {
    const params = {
      TableName: `${STORE_PREFIX}Brief`,
      Key: {
        Type: type,
        IID: iid,
      },
      ReturnConsumedCapacity: 'TOTAL',
    };
    console.log(`${params.TableName}.GETBRIEF: ${type} ${iid}`);
    resolve(docClient.get(params).promise());
  });
};

// Deletes full content by id
// Returns a promise
store.delete = function deleteItem(id) {
  return new Promise(resolve => {
    const params = {
      TableName: `${STORE_PREFIX}Content`,
      Key: {
        ID: id,
      },
      ReturnConsumedCapacity: 'TOTAL',
    };
    console.log(`${params.TableName}.DELETE: ${id}`);
    resolve(docClient.delete(params).promise());
  });
};

// Deletes brief content by type and iid
// Returns a promise
store.deleteBrief = function deleteBrief(type, iid) {
  return new Promise(resolve => {
    const params = {
      TableName: `${STORE_PREFIX}Brief`,
      Key: {
        Type: type,
        IID: iid,
      },
      ReturnConsumedCapacity: 'TOTAL',
    };
    console.log(`${params.TableName}.DELETEBRIEF: ${type} ${iid}`);
    resolve(docClient.delete(params).promise());
  });
};

// Gets a batch of brief data by Type and IIDs`
// Returns a promise
store.getBatchBrief = function getBatchBrief(type, iids) {
  return new Promise((resolve, reject) => {
    if (!iids || iids.length === 0) {
      console.log(`Skipping getBatchBrief: ${type}, ${iids} -- no iids to look for`);
      resolve({
        Items: [],
      });
      return;
    }

    console.log(`Started getBatchBrief: ${type}, ${iids}`);

    // Batch does not support duplicates, so we remove them
    const unique = Array.from(new Set(iids));

    const tableName = `${STORE_PREFIX}Brief`;
    const params = {
      RequestItems: {
        [tableName]: {
          Keys: unique.map(value => ({
            Type: type,
            IID: value,
          })),
        },
      },
      ReturnConsumedCapacity: 'TOTAL',
    };

    console.log(`${tableName}.BATCHGET: type=${type}, iids=${iids}`);
    docClient
      .batchGet(params)
      .promise()
      .then(result => {
        const output = {};
        if ('Responses' in result && tableName in result.Responses) {
          // Convert the results for the iids set to a list
          const reducer = (map, item) => Object.assign(map, { [item.IID]: item });
          const mapResult = result.Responses[tableName].reduce(reducer, {});
          output.Items = iids.map(iid => mapResult[iid]);
          output.Items = output.Items.filter(item => item); // filter falsey
        }
        output.ConsumedCapacity = result.ConsumedCapacity;
        output.UnprocessedKeys = result.UnprocessedKeys;
        resolve(output);
      })
      .catch(error => {
        reject(error);
      });
  });
};

// Gets a batch of full data for a list of IDs`
// Returns a promise
store.getBatch = function getBatch(ids) {
  return new Promise((resolve, reject) => {
    if (!ids || ids.length === 0) {
      console.log(`Skipping getBatch: ${ids} -- no ids to look for`);
      resolve({
        Items: [],
      });
      return;
    }

    console.log(`Started getBatch: ${ids}`);

    // Batch does not support duplicates, so we remove them
    const unique = Array.from(new Set(ids));

    const tableName = `${STORE_PREFIX}Content`;
    const params = {
      RequestItems: {
        [tableName]: {
          Keys: unique.map(value => ({
            ID: value,
          })),
        },
      },
      ReturnConsumedCapacity: 'TOTAL',
    };

    console.log(`${tableName}.BATCH: ids=${ids}`);
    docClient
      .batchGet(params)
      .promise()
      .then(result => {
        const output = {};
        if ('Responses' in result && tableName in result.Responses) {
          // Convert the results for the ids set to a list
          const reducer = (map, item) => Object.assign(map, { [item.ID]: item });
          const mapResult = result.Responses[tableName].reduce(reducer, {});
          output.Items = ids.map(id => mapResult[id]);
          output.Items = output.Items.filter(item => item); // filter falsey
        }
        output.ConsumedCapacity = result.ConsumedCapacity;
        output.UnprocessedKeys = result.UnprocessedKeys;
        resolve(output);
      })
      .catch(error => {
        reject(error);
      });
  });
};

// Query brief data by Type and TS
// Returns a promise
//
// Options accepted:
// - limit: integer; default 10
// - ascending: boolean; default computed from startDate and endDate
// - minTS: first timestamp for the type (inclusive)
// - maxTS: last timestamp for the type (inclusive)
// - startDate: a starting date for the type (inclusive)
// - endDate: an ending date for the type (inclusive)
// - filter: string; additional filters to be applied;
//                   e.g. filter=animalStatus:Available,Hold;-animalColor:Black
//           default undefined
//
// If minTS and maxTS are provided, then their values take
// precedence over startDate and endDate.
//
// If minTS or maxTS are not provided, and startDate or endDate
// are provided, then minTS and maxTS will be computed from the
// dates.
//
store.queryByTypeTS = function queryByTypeTS(type, options) {
  return new Promise((resolve, reject) => {
    console.log(`Started queryByTypeTS: ${type}`);

    const opts = Object.assign({ limit: 10 }, options);
    const limit = parseInt(opts.limit, 10);
    let { ascending } = opts;
    let { minTS, maxTS } = opts;
    let { startDate, endDate } = opts;
    const filter = filters.parse(opts.filter);

    startDate = utils.convertDate(startDate);
    endDate = utils.convertDate(endDate, startDate);

    console.log(`Decoded startDate: ${startDate ? startDate.format() : startDate}`);
    console.log(`Decoded endDate: ${endDate ? endDate.format() : endDate}`);

    const minMaxDate = utils.minMaxDate(startDate, endDate);
    const minDate = minMaxDate[0];
    const maxDate = minMaxDate[1];

    if (minTS === undefined && minDate) {
      minTS = utils.dateToTS(minDate);
    }

    if (maxTS === undefined && maxDate) {
      maxTS = utils.dateToTS(maxDate);
    }

    if (ascending === undefined) {
      if (startDate && endDate) {
        ascending = startDate.isBefore(endDate);
      } else {
        ascending = true;
      }
    } else {
      ascending = opts.ascending === true || opts.ascending === 'true' || opts.ascending === '1';
    }

    if (minTS !== undefined) {
      minTS = parseInt(minTS, 10);
    }

    if (maxTS !== undefined) {
      maxTS = parseInt(maxTS, 10);
    }

    const params = {
      TableName: `${STORE_PREFIX}Brief`,
      IndexName: 'TypeTS',
      ExpressionAttributeValues: {
        ':type': type,
      },
      ExpressionAttributeNames: {
        '#t': 'Type',
      },
      Limit: limit,
      ScanIndexForward: ascending,
      ReturnConsumedCapacity: 'INDEXES',
    };

    if (minTS !== undefined && maxTS !== undefined) {
      params.KeyConditionExpression = '(#t = :type) and (TS BETWEEN :minTS and :maxTS)';
      params.ExpressionAttributeValues[':minTS'] = minTS;
      params.ExpressionAttributeValues[':maxTS'] = maxTS;
    } else if (minTS !== undefined) {
      params.KeyConditionExpression = '(#t = :type) and (TS >= :minTS)';
      params.ExpressionAttributeValues[':minTS'] = minTS;
    } else if (maxTS !== undefined) {
      params.KeyConditionExpression = '(#t = :type) and (TS <= :maxTS)';
      params.ExpressionAttributeValues[':maxTS'] = maxTS;
    } else {
      params.KeyConditionExpression = '#t = :type';
    }

    console.log(`${params.TableName}.QUERY: type=${type}, minTS=${minTS}, maxTS=${maxTS}`);
    docClient
      .query(params)
      .promise()
      .then(result => {
        if (filter && filter.length) {
          const len1 = (result.Items && result.Items.length) || 0;
          Object.assign(result, { Items: filters.apply(result.Items, filter) });
          const len2 = (result.Items && result.Items.length) || 0;
          console.log(`Filtered queryByTypeTS: ${type} from ${len1} to ${len2} items`);
        }
        return result;
      })
      .then(result => {
        resolve(result);
      })
      .catch(error => {
        reject(error);
      });
  });
};

// Query brief data by Type and Featured
// Returns a promise
//
// Options accepted:
// - limit: integer; default 5
// - ascending: boolean
// - filter: string; additional filters to be applied;
//                   e.g. filter=animalStatus:Available,Hold;-animalColor:Black
//           default undefined
//
store.queryByTypeFeatured = function queryByTypeFeatured(type, options) {
  return new Promise((resolve, reject) => {
    console.log(`Started queryByTypeFeatured: ${type}`);

    const opts = Object.assign({ limit: 5, ascending: false }, options);
    const limit = parseInt(opts.limit, 10);
    const ascending =
      opts.ascending === true || opts.ascending === 'true' || opts.ascending === '1';
    const filter = filters.parse(opts.filter);

    const now = utils.now();

    const params = {
      TableName: `${STORE_PREFIX}Brief`,
      IndexName: 'TypeFeatured',
      KeyConditionExpression: '(#t = :type) and (FeatureDate <= :now)',
      ExpressionAttributeValues: {
        ':type': type,
        ':now': now,
      },
      ExpressionAttributeNames: {
        '#t': 'Type',
      },
      Limit: limit,
      ScanIndexForward: ascending,
      ReturnConsumedCapacity: 'INDEXES',
    };

    console.log(`${params.TableName}.QUERY: type=${type}, featureDate <= ${now}`);
    docClient
      .query(params)
      .promise()
      .then(result => {
        if (filter && filter.length) {
          const len1 = (result.Items && result.Items.length) || 0;
          Object.assign(result, { Items: filters.apply(result.Items, filter) });
          const len2 = (result.Items && result.Items.length) || 0;
          console.log(`Filtered queryByTypeFeatured: ${type} from ${len1} to ${len2} items`);
        }
        return result;
      })
      .then(result => {
        resolve(result);
      })
      .catch(error => {
        reject(error);
      });
  });
};

// Gets a list of items
// Returns a promise
//
// Options accepted:
// - start: integer; default 0
// - limit: integer; default 5
// - list: string; the id of a Content holding the IIDs to choose from
//         default undefined
// - ids: string; comma-separated list of IIDs to choose from
//        default undefined
// - random: boolean; if true then the items to return are picked randomly;
//                    `start` is ignored when picking randomly.
//           default false
// - full: boolean; if true then full items are being returned
//         default false
// - filter: string; additional filters to be applied;
//                   e.g. filter=animalStatus:Available,Hold;-animalColor:Black
//           default undefined
//
// You must specify either `list` or `ids` in the query.
store.getList = function getList(type, options) {
  return new Promise((resolve, reject) => {
    console.log(`Started getList: ${type}`);

    const opts = Object.assign(
      {
        start: 0,
        limit: 5,
        random: false,
        full: false,
      },
      options
    );
    const { start, limit, list, ids, random, full, filter } = {
      start: parseInt(opts.start, 10),
      limit: parseInt(opts.limit, 10),
      list: opts.list,
      ids: opts.ids,
      random: opts.random === true || opts.random === 'true' || opts.random === '1',
      full: opts.full === true || opts.full === 'true' || opts.full === '1',
      filter: filters.parse(opts.filter),
    };

    let promiseIds;

    // Take a hold of the ids from which to select the items
    if (ids) {
      promiseIds = Promise.resolve(ids.split(',').map(id => id.trim()));
    } else if (list) {
      promiseIds = store.get(list).then(result => _.get(result, `Item.Data.${type}`, []));
    } else {
      return reject(new Error('You must specify `list` or `ids` args'));
    }

    const result = promiseIds
      .then(allIds => {
        if (random) {
          return allIds.length <= limit ? _.shuffle(allIds) : pickRandom(allIds, { count: limit });
        }
        return _.slice(allIds, start, start + limit);
      })
      .then(selectedIds => {
        if (full) {
          return store.getBatch(selectedIds);
        }
        return store.getBatchBrief(
          type,
          selectedIds.map(id => {
            // Ignore possible type prefix (e.g. "article#") from ids
            if (id && id.startsWith(type + '#')) {
              return id.substring(type.length + 1);
            }
            return id;
          })
        );
      })
      .then(data => {
        if (filter && filter.length) {
          if (full) {
            // Add an extra deeper level for comparison
            _.forEach(filter, f => {
              f.key = 'Data.' + f.key;
            });
          }
          const len1 = (data.Items && data.Items.length) || 0;
          Object.assign(data, { Items: filters.apply(data.Items, filter) });
          const len2 = (data.Items && data.Items.length) || 0;
          console.log(`Filtered getList: ${type} from ${len1} to ${len2} items`);
        }
        return data;
      });
    resolve(result);
  });
};

// Gets a random list of items
// Returns a promise
//
// Options accepted:
// - limit: integer; default 5
// - list: string; the id of a Content holding the IIDs to choose from
//         default undefined
// - ids: string; comma-separated list of IIDs to choose from
//        default undefined
// - full: boolean; if true then full items are being returned
//         default false
// - filter: string; additional filters to be applied;
//                   e.g. filter=animalStatus:Available,Hold;-animalColor:Black
//           default undefined
//
// You must specify either `list` or `ids` in the query.
store.getRandomList = function getRandomList(type, options) {
  return store.getList(type, _.assign({}, options, { random: true }));
};

// Scans an entire DynamoDB table
// Returns an async generator
//
// tableName: string; DynamoDB table name to scan
//
// Options accepted:
// - pageSize: integer; how many items to retrieve at a time internally
//             default 10;
// - throttle: integer; the minimum wait between db calls to retrieve the data from DynamoDB
//             default 1000
// - params: object; additinal parameters to be applied to the scan operation (e.g. filters)
//           default undefined
store.scanTable = async function* scanTable(tableName, options) {
  const opts = Object.assign(
    {
      pageSize: 10,
      throttle: 1000,
    },
    options
  );

  const params = {
    TableName: tableName,
    ...opts.params,
    Limit: opts.pageSize,
    ReturnConsumedCapacity: 'TOTAL',
  };

  let lastCallAt = null;
  let page = 0;
  let total = 0;
  let totalScanned = 0;

  do {
    if (opts.throttle > 0 && lastCallAt !== null) {
      const now = Date.now();
      if (lastCallAt + opts.throttle > now) {
        const waitTime = lastCallAt + opts.throttle - now;
        console.log(`${params.TableName}.SCAN: sleep=${waitTime}ms`);
        await sleep(waitTime);
      }
    }

    console.log(`${params.TableName}.SCAN: page=${page}, pageSize=${opts.pageSize}`);
    lastCallAt = Date.now();
    const result = await docClient.scan(params).promise();

    total += result.Count;
    totalScanned += result.ScannedCount;

    console.log(
      JSON.stringify({
        Page: page,
        Total: total,
        TotalScanned: totalScanned,
        ..._.pick(result, ['Count', 'ScannedCount', 'LastEvaluatedKey', 'ConsumedCapacity']),
      })
    );

    for (const item of result.Items) {
      yield item;
    }

    page += 1;
    params.ExclusiveStartKey = result.LastEvaluatedKey;
  } while (params.ExclusiveStartKey);
};

// Scans the entire table of brief items
// Returns an async generator
//
// Options accepted:
// - pageSize: integer; how many items to retrieve at a time internally
//             default 10;
// - throttle: integer; the minimum wait between db calls to retrieve the data from DynamoDB
//             default 1000
store.scanBrief = function scanBrief(options) {
  const opts = Object.assign(
    {
      pageSize: 10,
      throttle: 1000,
    },
    options
  );

  return store.scanTable(`${STORE_PREFIX}Brief`, opts);
};

// Scans the entire table of full content items
// Returns an async generator
//
// Options accepted:
// - pageSize: integer; how many items to retrieve at a time internally
//             default 1;
// - throttle: integer; the minimum wait between db calls to retrieve the data from DynamoDB
//             default 1000
store.scan = function scan(options) {
  const opts = Object.assign(
    {
      pageSize: 1,
      throttle: 1000,
    },
    options
  );

  return store.scanTable(`${STORE_PREFIX}Content`, opts);
};

// Queries the entire table of brief items of a certain type.
// Returns an async generator
//
// This operation is paginatinated and will yield oll matching
// results. There is no limit specified by default.
//
// Options accepted:
// - prefix: string; optional prefix for IID
//           default undefined
// - ascending: boolean; if true results will be returned in ascending order of IID
//              default true
// - pageSize: integer; how many items to retrieve at a time internally
//             default 10;
// - throttle: integer; the minimum wait between db calls to retrieve the data from DynamoDB
//             default 1000
// - params: object; additinal parameters to be applied to the query operation (e.g. filters)
//           default undefined
store.queryByType = async function* queryByType(type, options) {
  console.log(`Started queryByType: ${type}`);

  const opts = Object.assign(
    {
      ascending: true,
      pageSize: 10,
      throttle: 1000,
    },
    options
  );

  const { prefix, ascending, pageSize, throttle } = opts;

  const params = {
    TableName: `${STORE_PREFIX}Brief`,
    ...opts.params,
    ExpressionAttributeValues: {
      ':type': type,
    },
    ExpressionAttributeNames: {
      '#t': 'Type',
    },
    Limit: pageSize,
    ScanIndexForward: ascending,
    ReturnConsumedCapacity: 'TOTAL',
  };

  if (prefix) {
    params.KeyConditionExpression = '(#t = :type) and begins_with(IID, :prefix)';
    params.ExpressionAttributeValues[':prefix'] = prefix;
  } else {
    params.KeyConditionExpression = '#t = :type';
  }

  let lastCallAt = null;
  let page = 0;
  let total = 0;
  let totalScanned = 0;

  do {
    if (throttle > 0 && lastCallAt !== null) {
      const now = Date.now();
      if (lastCallAt + throttle > now) {
        const waitTime = lastCallAt + throttle - now;
        console.log(
          `${params.TableName}.QUERY: type=${type}, prefix=${prefix}, sleep=${waitTime}ms`
        );
        await sleep(waitTime);
      }
    }

    console.log(
      `${params.TableName}.QUERY: type=${type}, prefix=${prefix}, page=${page}, pageSize=${pageSize}`
    );
    lastCallAt = Date.now();
    const result = await docClient.query(params).promise();

    total += result.Count;
    totalScanned += result.ScannedCount;

    console.log(
      JSON.stringify({
        Page: page,
        Total: total,
        TotalScanned: totalScanned,
        ..._.pick(result, ['Count', 'ScannedCount', 'LastEvaluatedKey', 'ConsumedCapacity']),
      })
    );

    for (const item of result.Items) {
      yield item;
    }

    page += 1;
    params.ExclusiveStartKey = result.LastEvaluatedKey;
  } while (params.ExclusiveStartKey);
};

module.exports = store;
