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

from contextlib import suppress
from unittest import mock

import pytest
from openlineage.client.facet import SchemaDatasetFacet, SchemaField
from openlineage.client.run import Dataset
from openlineage.common.models import DbColumn
from openlineage.common.sql import DbTableMeta

from airflow import DAG
from airflow.models import Connection
from airflow.providers.openlineage.extractors.postgres_extractor import PostgresExtractor
from airflow.providers.openlineage.utils.utils import get_connection
from airflow.utils.dates import days_ago
from airflow.utils.module_loading import import_string
from airflow.utils.session import create_session

with suppress(ImportError):
    PostgresOperator = import_string("airflow.providers.postgres.operators.postgres.PostgresOperator")
with suppress(ImportError):
    PostgresHook = import_string("airflow.providers.postgres.hooks.postgres.PostgresHook")

CONN_ID = "food_delivery_db"
CONN_URI = "postgres://user:pass@localhost:5432/food_delivery"
CONN_URI_WITHOUT_USERPASS = "postgres://localhost:5432/food_delivery"

DB_NAME = "food_delivery"
DB_SCHEMA_NAME = "public"
DB_TABLE_NAME = DbTableMeta("discounts")
DB_TABLE_COLUMNS = [
    DbColumn(name="id", type="int4", ordinal_position=1),
    DbColumn(name="amount_off", type="int4", ordinal_position=2),
    DbColumn(name="customer_email", type="varchar", ordinal_position=3),
    DbColumn(name="starts_on", type="timestamp", ordinal_position=4),
    DbColumn(name="ends_on", type="timestamp", ordinal_position=5),
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

SQL = f"SELECT * FROM {DB_TABLE_NAME.name};"

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
TASK = PostgresOperator(task_id=TASK_ID, postgres_conn_id=CONN_ID, sql=SQL, dag=DAG, database=DB_NAME)


@mock.patch("airflow.providers.openlineage.extractors.sql_extractor.get_table_schemas")
@mock.patch("airflow.hooks.base.BaseHook.get_connection")
def test_extract(get_connection, mock_get_table_schemas):
    table_name = f"{DB_NAME}.{DB_SCHEMA_NAME}.{DB_TABLE_NAME.name}"
    mock_get_table_schemas.return_value = (
        [Dataset(namespace="postgres://localhost:5432", name=table_name, facets={"schema": SCHEMA_FACET})],
        [],
    )

    conn = Connection(
        conn_id=CONN_ID, conn_type="postgres", host="localhost", port=5432, schema="food_delivery"
    )

    get_connection.return_value = conn

    expected_inputs = [
        Dataset(namespace="postgres://localhost:5432", name=table_name, facets={"schema": SCHEMA_FACET})
    ]

    task_metadata = PostgresExtractor(TASK).extract()

    assert task_metadata.inputs == expected_inputs
    assert task_metadata.outputs == []


def test_get_connection_import_returns_none_if_not_exists():
    assert get_connection("does_not_exist") is None
    assert get_connection("does_exist") is None


@pytest.fixture
def create_connection():
    conn = Connection("does_exist", conn_type="postgres")
    with create_session() as session:
        session.add(conn)
        session.commit()

    yield conn

    with create_session() as session:
        session.delete(conn)
        session.commit()


def test_get_connection_returns_one_if_exists(create_connection):
    conn = Connection("does_exist")
    assert get_connection("does_exist").conn_id == conn.conn_id
