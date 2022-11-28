import { BigQuery } from '@google-cloud/bigquery';
import { DUMMY_EXECUTE_COMMON_PARAMETERS, DUMMY_GOOGLE_SERVICE_ACCOUNT } from '@superblocksteam/shared-backend';
import BigqueryPlugin from '.';

jest.mock('@google-cloud/bigquery');

const DUMMY_ROWS = [
  {
    first_name: 'Jordan',
    last_name: 'Lownsbrough',
    email: 'jlownsbrough3@google.com.hk',
    ip_address: '96.231.220.84'
  },
  {
    first_name: 'Cecil',
    last_name: 'Hovie',
    email: 'chovie5@businesswire.com',
    ip_address: '155.165.198.68'
  },
  {
    first_name: 'Bernarr',
    last_name: 'Fabry',
    email: 'bfabry6@chicagotribune.com',
    ip_address: '161.210.5.105'
  },
  {
    first_name: 'Burl',
    last_name: 'Weatherill',
    email: 'bweatherill7@va.gov',
    ip_address: '223.46.75.47'
  },
  {
    first_name: 'Chuck',
    last_name: 'Dressel',
    email: 'cdressela@taobao.com',
    ip_address: '4.140.248.41'
  }
];

const DUMMY_METADATA = [
  {
    tableReference: {
      datasetId: 'SuperblocksFancyDataset',
      tableId: 'TheBestTable'
    },
    schema: {
      fields: [
        {
          name: 'first_name',
          type: 'STRING'
        },
        {
          name: 'last_name',
          type: 'STRING'
        },
        {
          name: 'email',
          type: 'STRING'
        },
        {
          name: 'ip_address',
          type: 'STRING'
        }
      ]
    }
  }
];

const EXPECTED_METADATA = {
  dbSchema: {
    tables: [
      {
        name: 'SuperblocksFancyDataset.TheBestTable',
        type: 'TABLE',
        columns: [
          { name: 'first_name', type: 'STRING' },
          { name: 'last_name', type: 'STRING' },
          { name: 'email', type: 'STRING' },
          { name: 'ip_address', type: 'STRING' }
        ]
      }
    ]
  }
};

beforeEach(() => {
  jest.resetAllMocks();
});

describe('bigquery execute', () => {
  test('execute: happy path scenario', async () => {
    const createQueryJobMock = jest.fn();
    const getQueryResultsMock = jest.fn().mockReturnValueOnce([DUMMY_ROWS]);
    const jobMock = { getQueryResults: getQueryResultsMock };
    createQueryJobMock.mockReturnValueOnce([jobMock]);
    jest.spyOn(BigQuery.prototype, 'createQueryJob').mockImplementation(() => {
      return [jobMock];
    });
    const plugin: BigqueryPlugin = new BigqueryPlugin();
    const executeResult = await plugin.execute({
      ...DUMMY_EXECUTE_COMMON_PARAMETERS,
      datasourceConfiguration: {
        authentication: {
          custom: {
            googleServiceAccount: { value: JSON.stringify(DUMMY_GOOGLE_SERVICE_ACCOUNT) }
          }
        }
      },
      actionConfiguration: {
        body: 'SELECT first_name, last_name, email, ip_address FROM `superblocks-XXX.SuperblocksFancyDataset.TheBestTable` LIMIT 5'
      }
    });
    expect(executeResult).toEqual({
      log: [],
      output: DUMMY_ROWS
    });
  });
});

describe('bigquery metadata', () => {
  test('metadata: happy path scenario', async () => {
    const getMetadataMock = jest.fn().mockReturnValueOnce(DUMMY_METADATA);
    const tables = [
      {
        getMetadata: getMetadataMock
      }
    ];
    const getTablesMock = jest.fn().mockReturnValueOnce([tables]);
    jest.spyOn(BigQuery.prototype, 'getDatasets').mockImplementation(() => {
      return [[{ getTables: getTablesMock }]];
    });
    const plugin: BigqueryPlugin = new BigqueryPlugin();
    const metadataResult = await plugin.metadata({
      authentication: {
        custom: {
          googleServiceAccount: { value: JSON.stringify(DUMMY_GOOGLE_SERVICE_ACCOUNT) }
        }
      }
    });
    expect(metadataResult).toEqual(EXPECTED_METADATA);
  });
});

describe('bigquery test', () => {
  test('test: happy path scenario', async () => {
    const getQueryResultsMock = jest.fn();
    const createQueryJobMock = jest.spyOn(BigQuery.prototype, 'createQueryJob').mockImplementation(() => {
      return [{ getQueryResults: getQueryResultsMock }];
    });
    const plugin: BigqueryPlugin = new BigqueryPlugin();
    await plugin.test({
      authentication: {
        custom: {
          googleServiceAccount: { value: JSON.stringify(DUMMY_GOOGLE_SERVICE_ACCOUNT) }
        }
      }
    });
    expect(getQueryResultsMock).toBeCalledTimes(1);
    expect(createQueryJobMock.mock.calls[0][0]).toEqual({ query: 'SELECT 1' });
  });
});
