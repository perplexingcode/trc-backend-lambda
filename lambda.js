import {
  DynamoDBClient,
  GetItemCommand,
  PutItemCommand,
  DeleteItemCommand,
  ScanCommand,
  QueryCommand,
} from '@aws-sdk/client-dynamodb';
import { marshall, unmarshall } from '@aws-sdk/util-dynamodb';

const DDB = new DynamoDBClient({
  region: 'ap-southeast-1',
  credentials: {
    accessKeyId: 'AKIAZO67ZV6JQAF5G2PD',
    secretAccessKey: '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',
  },
});

export const handler = async (event) => {
  console.log('event.routeKey:', event.routeKey);
  console.log('event.pathParameters:', event.pathParameters);
  console.log('event.body:', event.body);
  
  let batch1Id = null;

  async function asyWrap(request) {
    try {
      const data = await request;
      return data;
      // return unmarshall(data.Item);
    } catch (error) {
      console.error(error);
    }
  }

  async function baseScan(params) {

  
    let lastPage = false;
    let items = [];
    let batchId = null;

    const batchData = await asyWrap(DDB.send(new ScanCommand(params)));
    console.log('batch',batchData); 
    items = items.concat(batchData.Items);

    if (!batchData?.LastEvaluatedKey) {
      lastPage = true;
      const data = batchData.Items.map((item) => unmarshall(item));
      return data;
    }
    if (!batch1Id) batch1Id = batchData.LastEvaluatedKey.id.S;
    batchId = batchData.LastEvaluatedKey.id.S;
    while (!lastPage) {
      params.LastEvaluatedKey = batchId;
      const nextBatchData = await asyWrap(DDB.send(new ScanCommand(params)));
     console.log('next-batch',nextBatchData); 
      if (!nextBatchData.LastEvaluatedKey) break;
      params.LastEvaluatedKey = nextBatchData.LastEvaluatedKey;
      
      if (nextBatchData.LastEvaluatedKey.id.S == batch1Id) break;
      batchId = nextBatchData.LastEvaluatedKey;
      
      items = items.concat(nextBatchData.Items);
    }
    const data = items.map((item) => unmarshall(item));
    return data;
  }
  
  async function baseQuery(params) {
    const data = await asyWrap(DDB.send(new QueryCommand(params)));
    return data.Items.map((item) => unmarshall(item));
  }
  
  async function queryMany(paramsArray) {
    const promises = paramsArray.map(async (params) => {
      const data = await baseQuery(params);
      return data;
    });
    return Promise.all(promises);
  }

  switch (event.routeKey) {
    case 'GET /all/{table}': {
      const params = { TableName: event.pathParameters.table };
      const data = await baseScan(params);
      return data;
    }
    
    case 'GET /sample/{table}/{limit}': {
      const params = { TableName: event.pathParameters.table, Limit: +event.pathParameters.limit };
      const data = await baseScan(params);
      return data;
    }
    
    case "GET /all/{table}/{projection}": {
      const params = {
        TableName: event.pathParameters.table,
        ProjectionExpression: "#p",
        ExpressionAttributeNames: {
          "#p": event.pathParameters.projection,
        },
      };
      const data = await baseScan(params);
      return data;
    }
    
    case 'GET /id/{table}/{id}': {
      const params = {
        TableName: event.pathParameters.table,
        Key: { id: { S: event.pathParameters.id }, },
      };
      const data = await asyWrap(DDB.send(new GetItemCommand(params)));
      return unmarshall(data.Item);
    }
    
    case 'GET /id/{table}/{id}/{projection}': {
      const params = {
        TableName: event.pathParameters.table,
        Key: { id: { S: event.pathParameters.id }, },
        ProjectionExpression: '#p',
        ExpressionAttributeNames: {
          '#p': event.pathParameters.projection
        }
      };
      const data = await asyWrap(DDB.send(new GetItemCommand(params)));
      return  unmarshall(data.Item);
    }

    case 'GET /query/{table}/{key}/{value}': {
      const params = {
        TableName: event.pathParameters.table,
        IndexName: event.pathParameters.key + '-index',
        KeyConditionExpression: '#key = :pk',
        ExpressionAttributeValues: {
          ':pk': { S: event.pathParameters.value },
        },
        ExpressionAttributeNames: {
          '#key': event.pathParameters.key,
        },
      };
      const data = await asyWrap(DDB.send(new QueryCommand(params)));
      return data.Items.map((item) => unmarshall(item));
    }
    
     case 'POST /query/{table}/{key}': {
      const valuesToQuery = event.body ? JSON.parse(event.body) : [];
      const paramsArray = valuesToQuery.map((value) => ({
        TableName: event.pathParameters.table,
        IndexName: event.pathParameters.key + '-index',
        KeyConditionExpression: '#key = :pk',
        ExpressionAttributeValues: {
          ':pk': { S: value },
        },
        ExpressionAttributeNames: {
          '#key': event.pathParameters.key,
        },
      }));
      const data = await queryMany(paramsArray);
      return data;
    }

    case 'POST /upsert/{table}': {
      const params = {
        TableName: event.pathParameters.table,
        Item: marshall(JSON.parse(event.body)),
      };
      await asyWrap(DDB.send(new PutItemCommand(params)));
    }

    case 'POST /delete/{table}': {
      let data = JSON.parse(event.body);
      if (typeof data == 'string') data = [data];
      for (let i = 0; i < data.length; i++) {
        const params = {
          TableName: event.pathParameters.table,
          Key: {
            id: { S: data[i] },
          },
        };
        await asyWrap(DDB.send(new DeleteItemCommand(params)));
      }
    }
  }
};
