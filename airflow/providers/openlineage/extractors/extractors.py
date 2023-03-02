# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import os

from airflow.providers.openlineage.extractors.base import BaseExtractor, DefaultExtractor
from airflow.providers.openlineage.extractors.sql_check_extractors import get_check_extractors
from airflow.providers.openlineage.extractors.sql_execute_query import (
    get_sql_execute_query_extractor,
    sql_extractors,
)
from airflow.providers.openlineage.utils import try_import_from_string

_extractors: list[BaseExtractor] = list(
    filter(
        lambda t: t is not None,
        [
            try_import_from_string(
                'airflow.providers.openlineage.extractors.postgres_extractor.PostgresExtractor'
            ),
            try_import_from_string(
                'airflow.providers.openlineage.extractors.mysql_extractor.MySqlExtractor'
            ),
            try_import_from_string(
                'airflow.providers.openlineage.extractors.bigquery_extractor.BigQueryExtractor'
            ),
            try_import_from_string(
                'airflow.providers.openlineage.extractors.great_expectations_extractor.GreatExpectationsExtractor'  # noqa: E501
            ),
            try_import_from_string(
                'airflow.providers.openlineage.extractors.snowflake_extractor.SnowflakeExtractor'
            ),
            try_import_from_string(
                'airflow.providers.openlineage.extractors.python_extractor.PythonExtractor'
            ),
            try_import_from_string(
                'airflow.providers.openlineage.extractors.bash_extractor.BashExtractor'
            ),
            try_import_from_string(
                'airflow.providers.openlineage.extractors.redshift_sql_extractor.RedshiftSQLExtractor'
            ),
            try_import_from_string(
                'airflow.providers.openlineage.extractors.redshift_data_extractor.RedshiftDataExtractor'
            ),
            try_import_from_string(
                'airflow.providers.openlineage.extractors.trino_extractor.TrinoExtractor'
            ),
            try_import_from_string(
                'airflow.providers.openlineage.extractors.athena_extractor.AthenaExtractor'
            ),
            try_import_from_string(
                'airflow.providers.openlineage.extractors.sftp_extractor.SFTPExtractor'
            ),
            try_import_from_string(
                'airflow.providers.openlineage.extractors.ftp_extractor.FTPExtractor'
            ),
            try_import_from_string(
                'airflow.providers.openlineage.extractors.sagemaker_extractors.SageMakerProcessingExtractor'
            ),
            try_import_from_string(
                'airflow.providers.openlineage.extractors.sagemaker_extractors.SageMakerTrainingExtractor'
            ),
            try_import_from_string(
                'airflow.providers.openlineage.extractors.sagemaker_extractors.SageMakerTransformExtractor'
            ),
            try_import_from_string(
                'airflow.providers.openlineage.extractors.s3_extractor.S3CopyObjectExtractor'
            ),
            try_import_from_string(
                'airflow.providers.openlineage.extractors.s3_extractor.S3FileTransformExtractor'
            ),
            try_import_from_string(
                'airflow.providers.openlineage.extractors.gcs_extractor.GCSToGCSExtractor'
            ),
        ],
    )
)

_extractors += get_check_extractors(
    try_import_from_string(
        'airflow.providers.openlineage.extractors.sql_extractor.SqlExtractor'
    )
)

_check_providers = {
    "PostgresExtractor": "postgres",
    "MySqlExtractor": "mysql",
    "BigQueryExtractor": ["gcpbigquery", "google_cloud_platform"],
    "SnowflakeExtractor": "snowflake",
    "TrinoExtractor": "trino",
    "AthenaExtractor": "aws",
    "SFTPExtractor": ["sftp", "ssh"],
    "FTPExtractor": "ftp",
}

class Extractors:
    """
    This exposes implemented extractors, while hiding ones that require additional, unmet
    dependency. Patchers are a category of extractor that needs to hook up to operator's
    internals during DAG creation.
    """

    def __init__(self):
        # Do not expose extractors relying on external dependencies that are not installed
        self.extractors = {}
        self.default_extractor = DefaultExtractor

        for extractor in _extractors:
            for operator_class in extractor.get_operator_classnames():
                self.extractors[operator_class] = extractor

        # Comma-separated extractors in OPENLINEAGE_EXTRACTORS variable.
        # Extractors should implement BaseExtractor
        from airflow.providers.openlineage.utils import import_from_string
        
        env_extractors = os.getenv("OPENLINEAGE_EXTRACTORS")
        if env_extractors is not None:
            for extractor in env_extractors.split(';'):
                extractor: BaseExtractor = import_from_string(extractor.strip())
                for operator_class in extractor.get_operator_classnames():
                    self.extractors[operator_class] = extractor

    def add_extractor(self, operator: str, extractor: type):
        self.extractors[operator] = extractor

    def get_extractor_class(self, clazz: type) -> type[BaseExtractor] | None:
        name = clazz.__name__
        if name in self.extractors:
            return self.extractors[name]

        def method_exists(method_name):
            method = getattr(clazz, method_name, None)
            if method:
                return callable(method)

        if method_exists("get_openlineage_facets_on_start") or method_exists(
            "get_openlineage_facets_on_complete"
        ):
            return self.default_extractor
        return None

    def instantiate_abstract_extractors(self, task) -> None:
        # instantiate sql check extractors
        from airflow.hooks.base import BaseHook

        if task.__class__.__name__ in (
            "SQLCheckOperator", "SQLValueCheckOperator", "SQLThresholdCheckOperator",
            "SQLIntervalCheckOperator", "SQLColumnCheckOperator", "SQLTableCheckOperator",
            "BigQueryTableCheckOperator", "BigQueryColumnCheckOperator"
        ):
            for extractor in self.extractors.values():
                conn_type = _check_providers.get(extractor.__name__, "")
                task_conn_type = None
                if hasattr(task, "gcp_conn_id"):
                    task_conn_type = BaseHook.get_connection(task.gcp_conn_id).conn_type
                elif hasattr(task, "conn_id"):
                    task_conn_type = BaseHook.get_connection(task.conn_id).conn_type
                if task_conn_type in conn_type:
                    check_extractors = get_check_extractors(extractor)
                    for check_extractor in check_extractors:
                        for operator_class in check_extractor.get_operator_classnames():
                            self.extractors[operator_class] = check_extractor
                    else:
                        return
            else:
                raise ValueError(
                    "Extractor for the given task's conn_type (%s) does not exist.",
                    task_conn_type
                )

        # instantiate sql execute query extractor
        if task.__class__.__name__ == "SQLExecuteQueryOperator":
            task_conn_type = BaseHook.get_connection(task.conn_id).conn_type
            extractor_name = sql_extractors.get(task_conn_type, None)
            extractor = list(
                filter(lambda x: x.__name__ == extractor_name, self.extractors.values())
            )
            if extractor:
                self.extractors[
                    "SQLExecuteQueryOperator"
                ] = get_sql_execute_query_extractor(extractor[0])
        return