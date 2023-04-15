const AWS = require('aws-sdk');
const fs = require('fs');

module.exports = function loadFixtures() {
  const DYNAMO_DB_ENDPOINT = process.env.DYNAMO_DB_ENDPOINT || null;
  const STORE_REGION = process.env.STORE_REGION || 'us-east-1';
  const STORE_PREFIX = process.env.STORE_PREFIX || 'Dev_';

  AWS.config.update({
    region: STORE_REGION,
  });

  const dynamodb = new AWS.DynamoDB({ endpoint: DYNAMO_DB_ENDPOINT });
  const docClient = new AWS.DynamoDB.DocumentClient({
    service: dynamodb,
    convertEmptyValues: true,
  });

  console.log('Importing content into DynamoDB. Please wait.');

  const allData = JSON.parse(fs.readFileSync(`${__dirname}/fixtures.json`, 'utf8'));
  const promises = [];

  allData.Content.forEach(content => {
    const now = new Date().toISOString();
    const params = {
      TableName: `${STORE_PREFIX}Content`,
      Item: {
        ID: content.ID,
        Data: content.Data,
        CreatedAt: now,
        CreatedBy: 'fixture',
      },
    };
    console.log(`Preparing: ${content.ID}...`);
    const promise = docClient
      .put(params)
      .promise()
      .then(() => {
        console.log(`PutItem succeeded [${params.TableName}]: ${content.ID}`);
      })
      .catch(error => {
        console.error(
          'Unable to add content',
          content.ID,
          '. Error JSON:',
          error
        );
      });
    promises.push(promise);
  });

  allData.Brief.forEach(content => {
    const params = {
      TableName: `${STORE_PREFIX}Brief`,
      Item: content,
    };
    console.log(`Preparing ${content.Type}: ${content.IID}...`);
    const promise = docClient
      .put(params)
      .promise()
      .then(() => {
        console.log(`PutItem succeeded [${params.TableName}]: ${content.Type}#${content.IID}`);
      })
      .catch(error => {
        console.error(
          'Unable to add content',
          content.Type,
          '#',
          content.IID,
          '. Error JSON:',
          error
        );
      });
    promises.push(promise);
  });

  return Promise.all(promises);
};
