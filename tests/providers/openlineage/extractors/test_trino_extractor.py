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
from openlineage.common.models import DbColumn, DbTableSchema
from openlineage.common.sql import DbTableMeta

from airflow import DAG
from airflow.models import Connection
from airflow.providers.openlineage.extractors.trino_extractor import TrinoExtractor
from airflow.utils.dates import days_ago
from airflow.utils.module_loading import import_string

with suppress(ImportError):
    TrinoOperator = import_string("airflow.providers.trino.operators.trino.TrinoOperator")
with suppress(ImportError):
    TrinoHook = import_string("airflow.providers.trino.hooks.trino.TrinoHook")

CONN_ID = "food_delivery_db"
CONN_URI = "trino://user:pass@localhost:8080/food_delivery"
CONN_URI_WITHOUT_USERPASS = "trino://localhost:8080/food_delivery"

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
DB_TABLE_SCHEMA = DbTableSchema(
    schema_name=DB_SCHEMA_NAME, table_name=DB_TABLE_NAME, columns=DB_TABLE_COLUMNS
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


@pytest.mark.skipif(
    TrinoOperator is None, reason="TrinoOperator is available only with apache-airflow-providers-trino 3.1.0+"
)
@mock.patch("airflow.providers.openlineage.extractors.sql_extractor.get_table_schemas")
@mock.patch("airflow.hooks.base.BaseHook.get_connection")
def test_extract(get_connection, mock_get_table_schemas):
    schema_facet = SchemaDatasetFacet(
        fields=[
            SchemaField(name=field.name, type=field.type, description=field.description)
            for field in DB_TABLE_COLUMNS
        ]
    )

    mock_get_table_schemas.return_value = (
        [
            Dataset(
                namespace=CONN_URI_WITHOUT_USERPASS,
                name=f"{DB_NAME}.{DB_SCHEMA_NAME}.{DB_TABLE_NAME.name}",
                facets={"schema": schema_facet},
            )
        ],
        [],
    )

    conn = Connection(
        conn_id=CONN_ID,
        conn_type="trino",
        host="localhost",
        port="8080",
        schema="food_delivery",
        extra={"catalog": DB_NAME},
    )

    get_connection.return_value = conn

    expected_inputs = [
        Dataset(
            namespace=CONN_URI_WITHOUT_USERPASS,
            name=f"{DB_NAME}.{DB_SCHEMA_NAME}.{DB_TABLE_NAME.name}",
            facets={"schema": schema_facet},
        )
    ]

    task = TrinoOperator(
        task_id=TASK_ID,
        trino_conn_id=CONN_ID,
        sql=SQL,
        dag=DAG,
    )
    task_metadata = TrinoExtractor(task).extract()

    assert task_metadata.inputs == expected_inputs
    assert task_metadata.outputs == []
