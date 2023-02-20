# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from urllib.parse import urlparse

from openlineage.client.facet import SchemaField, SqlJobFacet
from openlineage.client.run import Dataset
from openlineage.common.sql import SqlMeta, parse

from airflow.providers.openlineage.plugins.extractors.base import BaseExtractor, TaskMetadata
from airflow.providers.openlineage.plugins.extractors.dbapi_utils import TableSchema
from airflow.providers.openlineage.plugins.extractors.sql_extractor import SqlExtractor


class AthenaExtractor(BaseExtractor):

    @classmethod
    def get_operator_classnames(cls) -> list[str]:
        return ["AthenaOperator", "AWSAthenaOperator"]

    def extract(self) -> TaskMetadata:
        task_name = f"{self.operator.dag_id}.{self.operator.task_id}"
        job_facets = {
            "sql": SqlJobFacet(query=SqlExtractor._normalize_sql(self.operator.query))
        }

        sql_meta: SqlMeta | None = parse(self.operator.query, "generic", None)
        inputs: list[Dataset] = list(filter(None, [
            self._get_inout_dataset(self.operator.database, table.name)
            for table in sql_meta.in_tables
        ])) if sql_meta and sql_meta.in_tables else []

        # Athena can output query result to a new table with CTAS query.
        # cf. https://docs.aws.amazon.com/athena/latest/ug/ctas.html
        outputs: list[Dataset] = list(filter(None, [
            self._get_inout_dataset(self.operator.database, table.name)
            for table in sql_meta.out_tables
        ])) if sql_meta and sql_meta.out_tables else []

        # In addition to CTAS query, it's also possible to specify output location on S3
        # with a mandatory parameter, OutputLocation in ResultConfiguration.
        # cf. https://docs.aws.amazon.com/athena/latest/APIReference/API_ResultConfiguration.html#athena-Type-ResultConfiguration-OutputLocation  # noqa: E501
        #
        # Depending on the query type and the external_location property in the CTAS query,
        # its behavior changes as follows:
        #
        # * Normal SELECT statement
        #   -> The result is put into output_location as files rather than a table.
        #
        # * CTAS statement without external_location (`CREATE TABLE ... AS SELECT ...`)
        #   -> The result is put into output_location as a table,
        #      that is, both metadata files and data files are in there.
        #
        # * CTAS statement with external_location
        #   (`CREATE TABLE ... WITH (external_location='s3://bucket/key') AS SELECT ...`)
        #   -> The result is output as a table, but metadata and data files are
        #      separated into output_location and external_location respectively.
        #
        # For the last case, output_location may be unnecessary as OL's output information,
        # but we keep it as of now since it may be useful for some purpose.
        output_location = self.operator.output_location
        parsed = urlparse(output_location)
        scheme = parsed.scheme,
        authority = parsed.netloc,
        namespace = f"{scheme}://{authority}"
        outputs.append(Dataset(
            namespace=namespace,
            name=parsed.path,
        ))

        return TaskMetadata(
            name=task_name,
            inputs=inputs,
            outputs=outputs,
            run_facets={},
            job_facets=job_facets,
        )

    def _get_inout_dataset(self, database, table) -> Dataset | None:
        # Currently, AthenaOperator and AthenaHook don't have a functionality to specify catalog,
        # and it seems to implicitly assume that the default catalog (AwsDataCatalog) is target.
        CATALOG_NAME = "AwsDataCatalog"

        # AthenaHook.get_conn() doesn't return PEP-249 compliant connection object,
        # but Boto3's Athena.Client instead. So this class doesn't inherit from
        # SqlExtractor, which depends on PEP-249 compliant connection.
        client = self.operator.hook.get_conn()
        try:
            table_metadata = client.get_table_metadata(
                CatalogName=CATALOG_NAME,
                DatabaseName=database,
                TableName=table
            )

            scheme = "awsathena"
            authority = f"athena.{client._client_config.region_name}.amazonaws.com"

            return TableSchema(
                table=table,
                schema=database,
                database=CATALOG_NAME,
                fields=[
                    SchemaField(name=column["Name"], type=column["Type"])
                    for i, column in enumerate(table_metadata["TableMetadata"]["Columns"])
                ]
            ).to_dataset(f"{scheme}://{authority}")
        except Exception as e:
            self.log.error(
                f"Cannot retrieve table metadata from Athena.Client. {e}", exc_info=True
            )
            return None
