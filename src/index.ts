import { BigQuery } from '@google-cloud/bigquery';
import {
  BigqueryActionConfiguration,
  BigqueryDatasourceConfiguration,
  DatasourceMetadataDto,
  ExecutionOutput,
  IntegrationError,
  NotFoundError,
  RawRequest,
  Table,
  TableType
} from '@superblocksteam/shared';
import { PluginExecutionProps, DatabasePlugin, CreateConnection } from '@superblocksteam/shared-backend';
import { isEmpty } from 'lodash';

export default class BigqueryPlugin extends DatabasePlugin {
  protected readonly useOrderedParameters = false;

  public async execute({
    context,
    datasourceConfiguration,
    actionConfiguration
  }: PluginExecutionProps<BigqueryDatasourceConfiguration>): Promise<ExecutionOutput> {
    const ret = new ExecutionOutput();
    const options = {
      query: actionConfiguration.body,
      params: context.preparedStatementContext
    };
    let client: BigQuery;
    try {
      client = await this.createConnection(datasourceConfiguration);
      if (isEmpty(actionConfiguration.body)) {
        return ret;
      }

      const [rows] = await this.executeQuery(async () => {
        const [job] = await client.createQueryJob(options);
        return job.getQueryResults();
      });

      ret.output = rows;
      return ret;
    } catch (err) {
      throw new IntegrationError(`BigQuery query failed: ${err}`);
    }
  }

  getRequest(actionConfiguration: BigqueryActionConfiguration): RawRequest {
    return actionConfiguration.body;
  }

  dynamicProperties(): string[] {
    return ['body'];
  }

  @CreateConnection
  private async createConnection(datasourceConfiguration: BigqueryDatasourceConfiguration): Promise<BigQuery> {
    if (!datasourceConfiguration) {
      throw new NotFoundError('No datasource found when creating BigQuery client');
    }
    try {
      const key = datasourceConfiguration.authentication?.custom?.googleServiceAccount?.value ?? '';
      const credentials = JSON.parse(key);
      const projectId = credentials['project_id'];
      const opts = { projectId, credentials };
      return new BigQuery(opts);
    } catch (err) {
      throw new IntegrationError('Could not parse credentials.');
    }
  }

  async metadata(datasourceConfiguration: BigqueryDatasourceConfiguration): Promise<DatasourceMetadataDto> {
    try {
      const client = await this.createConnection(datasourceConfiguration);
      const [datasets] = await this.executeQuery(() => {
        return client.getDatasets();
      });
      const entities: Table[] = [];
      for (const dataset of datasets) {
        const [tables] = await this.executeQuery(() => {
          return dataset.getTables();
        });
        for (const table of tables) {
          const tableMetadata = await this.executeQuery(() => {
            return table.getMetadata();
          });
          const fullTableName = `${tableMetadata[0].tableReference?.datasetId}.${tableMetadata[0].tableReference?.tableId}`;
          const tableEntity: Table = { name: fullTableName, type: TableType.TABLE, columns: [] };
          const fields = tableMetadata[0].schema?.fields;
          if (fields === undefined) {
            continue;
          }
          for (const column of fields) {
            tableEntity.columns.push({ name: column.name, type: column.type });
          }
          entities.push(tableEntity);
        }
      }
      return {
        dbSchema: { tables: entities }
      };
    } catch (err) {
      throw new IntegrationError(`Failed to connect to Big Query, ${err.message}`);
    }
  }

  async test(datasourceConfiguration: BigqueryDatasourceConfiguration): Promise<void> {
    let client;
    try {
      client = await this.createConnection(datasourceConfiguration);
      const options = { query: 'SELECT 1' };
      await this.executeQuery(async () => {
        const [job] = await client.createQueryJob(options);
        await job.getQueryResults();
      });
    } catch (err) {
      throw new IntegrationError(`Test Big Query connection failed, ${err.message}`);
    }
  }
}
