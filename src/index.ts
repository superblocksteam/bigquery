import { BigQuery } from '@google-cloud/bigquery';
import {
  BigqueryActionConfiguration,
  BigqueryDatasourceConfiguration,
  DatasourceMetadataDto,
  ExecutionOutput,
  IntegrationError,
  NotFoundError,
  RawRequest,
  ResolvedActionConfigurationProperty,
  Table,
  TableType
} from '@superblocksteam/shared';
import {
  ActionConfigurationResolutionContext,
  BasePlugin,
  PluginExecutionProps,
  resolveActionConfigurationPropertyUtil
} from '@superblocksteam/shared-backend';
import { isEmpty } from 'lodash';

export default class BigqueryPlugin extends BasePlugin {
  async resolveActionConfigurationProperty({
    context,
    actionConfiguration,
    files,
    property,
    escapeStrings
  }: // eslint-disable-next-line @typescript-eslint/no-explicit-any
  ActionConfigurationResolutionContext): Promise<ResolvedActionConfigurationProperty> {
    return resolveActionConfigurationPropertyUtil(
      super.resolveActionConfigurationProperty,
      {
        context,
        actionConfiguration,
        files,
        property,
        escapeStrings
      },
      false /* useOrderedParameters */
    );
  }

  async execute({
    context,
    datasourceConfiguration,
    actionConfiguration
  }: PluginExecutionProps<BigqueryDatasourceConfiguration>): Promise<ExecutionOutput> {
    try {
      const ret = new ExecutionOutput();
      const client = this.createClient(datasourceConfiguration);
      const options = {
        query: actionConfiguration.body,
        params: context.preparedStatementContext
      };
      if (isEmpty(actionConfiguration.body)) {
        return ret;
      }

      const [job] = await client.createQueryJob(options);
      const [rows] = await job.getQueryResults();

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

  createClient(datasourceConfiguration: BigqueryDatasourceConfiguration): BigQuery {
    if (!datasourceConfiguration) {
      throw new NotFoundError('No datasource found when creating BigQuery client');
    }
    const key = datasourceConfiguration.authentication?.custom?.googleServiceAccount?.value ?? '';
    const credentials = JSON.parse(key);
    const projectId = credentials['project_id'];

    const opts = { projectId, credentials };
    return new BigQuery(opts);
  }

  async metadata(datasourceConfiguration: BigqueryDatasourceConfiguration): Promise<DatasourceMetadataDto> {
    try {
      const client = this.createClient(datasourceConfiguration);
      const [datasets] = await client.getDatasets();
      const entities: Table[] = [];
      for (const dataset of datasets) {
        const [tables] = await dataset.getTables();
        for (const table of tables) {
          const tableMetadata = await table.getMetadata();
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
    try {
      const client = this.createClient(datasourceConfiguration);
      const options = { query: 'SELECT 1' };
      const [job] = await client.createQueryJob(options);
      await job.getQueryResults();
    } catch (err) {
      throw new IntegrationError(`Test Big Query connection failed, ${err.message}`);
    }
  }
}
