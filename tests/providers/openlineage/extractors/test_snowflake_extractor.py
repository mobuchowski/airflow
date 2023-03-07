# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from unittest import mock

from openlineage.client.facet import SchemaDatasetFacet, SchemaField
from openlineage.client.run import Dataset
from openlineage.common.models import DbColumn
from openlineage.common.sql import DbTableMeta

from airflow import DAG
from airflow.models import Connection
from airflow.providers.openlineage.extractors.snowflake_extractor import SnowflakeExtractor
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago

CONN_ID = "food_delivery_db"
CONN_URI = "snowflake://snowflake.example/db-schema?account=test_account&database=FOOD_DELIVERY&region=us-east&warehouse=snow-warehouse&secret=hideit"  # noqa
CONN_URI_WITH_EXTRA_PREFIX = "snowflake://snowflake.example/db-schema?extra__snowflake__account=test_account&extra__snowflake__database=FOOD_DELIVERY&extra__snowflake__region=us-east&extra__snowflake__warehouse=snow-warehouse&extra__snowflake__secret=hideit"  # noqa
CONN_URI_URIPARSED = "snowflake://snowflake.example/db-schema?account=%5B%27test_account%27%5D&database=%5B%27FOOD_DELIVERY%27%5D&region=%5B%27us-east%27%5D&warehouse=%5B%27snow-warehouse%27%5D"  # noqa

DB_NAME = "FOOD_DELIVERY"
DB_SCHEMA_NAME = "PUBLIC"
DB_TABLE_NAME = DbTableMeta("DISCOUNTS")
DB_TABLE_COLUMNS = [
    DbColumn(name="ID", type="int4", ordinal_position=1),
    DbColumn(name="AMOUNT_OFF", type="int4", ordinal_position=2),
    DbColumn(name="CUSTOMER_EMAIL", type="varchar", ordinal_position=3),
    DbColumn(name="STARTS_ON", type="timestamp", ordinal_position=4),
    DbColumn(name="ENDS_ON", type="timestamp", ordinal_position=5),
]
SCHEMA_FACET = SchemaDatasetFacet(
    fields=[
        SchemaField(name="ID", type="int4"),
        SchemaField(name="AMOUNT_OFF", type="int4"),
        SchemaField(name="CUSTOMER_EMAIL", type="varchar"),
        SchemaField(name="STARTS_ON", type="timestamp"),
        SchemaField(name="ENDS_ON", type="timestamp"),
    ]
)

SQL = f"SELECT * FROM {DB_NAME}.{DB_SCHEMA_NAME}.{DB_TABLE_NAME.name};"

DAG_ID = "email_discounts"
DAG_OWNER = "datascience"
DAG_DEFAULT_ARGS = {
    "owner": DAG_OWNER,
    "depends_on_past": False,
    "start_date": days_ago(7),
    "email_on_failure": False,
    "email_on_retry": False,
    "email": ["datascience@example.com"],
}
DAG_DESCRIPTION = "Email discounts to customers that have experienced order delays daily"

DAG = dag = DAG(
    DAG_ID, schedule_interval="@weekly", default_args=DAG_DEFAULT_ARGS, description=DAG_DESCRIPTION
)

TASK_ID = "select"
TASK = SnowflakeOperator(
    task_id=TASK_ID,
    snowflake_conn_id=CONN_ID,
    sql=SQL,
    dag=DAG,
)


def mock_get_hook(operator):
    mocked = mock.MagicMock()
    mocked.return_value.conn_name_attr = "snowflake_conn_id"
    mocked.return_value.get_uri.return_value = "snowflake://user:pass@xy123456/db/schema"
    if hasattr(operator, "get_db_hook"):
        operator.get_db_hook = mocked
    else:
        operator.get_hook = mocked


def get_hook_method(operator):
    if hasattr(operator, "get_db_hook"):
        return operator.get_db_hook
    else:
        return operator.get_hook


@mock.patch("airflow.providers.openlineage.extractors.sql_extractor.get_table_schemas")
@mock.patch("airflow.hooks.base.BaseHook.get_connection")
@mock.patch("airflow.providers.openlineage.extractors.snowflake_extractor.execute_query_on_hook")
def test_extract(execute_query_on_hook, get_connection, mock_get_table_schemas):
    execute_query_on_hook.return_value = "public"

    mock_get_table_schemas.return_value = (
        [
            Dataset(
                namespace="snowflake://test_account",
                name=f"{DB_NAME}.{DB_SCHEMA_NAME}.{DB_TABLE_NAME.name}",
                facets={"schema": SCHEMA_FACET},
            )
        ],
        [],
    )

    conn = Connection()
    conn.parse_from_uri(uri=CONN_URI)
    get_connection.return_value = conn

    mock_get_hook(TASK)

    get_hook_method(TASK).return_value._get_conn_params.return_value = {
        "account": "test_account",
        "database": DB_NAME,
    }

    expected_inputs = [
        Dataset(
            namespace="snowflake://test_account",
            name=f"{DB_NAME}.{DB_SCHEMA_NAME}.{DB_TABLE_NAME.name}",
            facets={"schema": SCHEMA_FACET},
        )
    ]

    task_metadata = SnowflakeExtractor(TASK).extract()

    assert task_metadata.inputs == expected_inputs
    assert task_metadata.outputs == []


@mock.patch("airflow.providers.openlineage.extractors.sql_extractor.get_table_schemas")
@mock.patch("airflow.hooks.base.BaseHook.get_connection")
@mock.patch("airflow.providers.openlineage.extractors.snowflake_extractor.execute_query_on_hook")
def test_extract_query_ids(execute_query_on_hook, get_connection, mock_get_table_schemas):
    mock_get_table_schemas.return_value = (
        [],
        [],
    )

    execute_query_on_hook.return_value = "public"

    conn = Connection()
    conn.parse_from_uri(uri=CONN_URI)
    get_connection.return_value = conn

    mock_get_hook(TASK)
    TASK.query_ids = ["1500100900"]

    task_metadata = SnowflakeExtractor(TASK).extract()

    assert task_metadata.run_facets["externalQuery"].externalQueryId == "1500100900"


@mock.patch("airflow.hooks.base.BaseHook.get_connection")
def test_information_schema_query(get_connection):
    extractor = SnowflakeExtractor(TASK)

    conn = Connection()
    conn.parse_from_uri(uri=CONN_URI)
    get_connection.return_value = conn

    same_db_explicit = [
        DbTableMeta("db.schema.table_a"),
        DbTableMeta("DB.SCHEMA.table_b"),
    ]

    same_db_explicit_sql = (
        "SELECT table_schema, table_name, column_name, ordinal_position, data_type "
        "FROM DB.information_schema.columns "
        "WHERE ( table_schema = 'SCHEMA' AND table_name IN ('TABLE_A','TABLE_B') );"
    )

    same_db_implicit = [DbTableMeta("SCHEMA.TABLE_A"), DbTableMeta("SCHEMA.TABLE_B")]

    same_db_implicit_sql = (
        "SELECT table_schema, table_name, column_name, ordinal_position, data_type "
        "FROM FOOD_DELIVERY.information_schema.columns "
        "WHERE ( table_schema = 'SCHEMA' AND table_name IN ('TABLE_A','TABLE_B') );"
    )

    different_databases_explicit = [
        DbTableMeta("DB_1.SCHEMA.TABLE_A"),
        DbTableMeta("DB_2.SCHEMA.TABLE_B"),
    ]

    different_databases_explicit_sql = (
        "SELECT table_schema, table_name, column_name, ordinal_position, data_type "
        "FROM DB_1.information_schema.columns "
        "WHERE ( table_schema = 'SCHEMA' AND table_name IN ('TABLE_A') ) "
        "UNION ALL "
        "SELECT table_schema, table_name, column_name, ordinal_position, data_type "
        "FROM DB_2.information_schema.columns "
        "WHERE ( table_schema = 'SCHEMA' AND table_name IN ('TABLE_B') );"
    )

    different_databases_mixed = [
        DbTableMeta("SCHEMA.TABLE_A"),
        DbTableMeta("DB_2.SCHEMA.TABLE_B"),
    ]

    different_databases_mixed_sql = (
        "SELECT table_schema, table_name, column_name, ordinal_position, data_type "
        "FROM FOOD_DELIVERY.information_schema.columns "
        "WHERE ( table_schema = 'SCHEMA' AND table_name IN ('TABLE_A') ) "
        "UNION ALL "
        "SELECT table_schema, table_name, column_name, ordinal_position, data_type "
        "FROM DB_2.information_schema.columns "
        "WHERE ( table_schema = 'SCHEMA' AND table_name IN ('TABLE_B') );"
    )

    assert extractor._information_schema_query(same_db_explicit) == same_db_explicit_sql
    assert extractor._information_schema_query(same_db_implicit) == same_db_implicit_sql
    assert (
        extractor._information_schema_query(different_databases_explicit) == different_databases_explicit_sql
    )
    assert extractor._information_schema_query(different_databases_mixed) == different_databases_mixed_sql
