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

import pytest
from openlineage.client.facet import SchemaDatasetFacet, SchemaField
from openlineage.client.run import Dataset
from openlineage.common.models import DbColumn
from openlineage.common.sql import DbTableMeta

from airflow import DAG
from airflow.models import Connection
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from airflow.providers.openlineage.extractors.redshift_sql_extractor import RedshiftSQLExtractor
from airflow.providers.openlineage.utils.utils import get_connection
from airflow.utils.dates import days_ago
from airflow.utils.session import create_session

CONN_ID = "food_delivery_db"
CONN_URI = "redshift://user:pass@redshift-cluster-name.id.region.redshift.amazonaws.com:5439" "/food_delivery"
CONN_URI_WITHOUT_USERPASS = (
    "redshift://redshift-cluster-name.id.region.redshift.amazonaws.com:5439/food_delivery"
)

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
        SchemaField(name="id", type="int4"),
        SchemaField(name="amount_off", type="int4"),
        SchemaField(name="customer_email", type="varchar"),
        SchemaField(name="starts_on", type="timestamp"),
        SchemaField(name="ends_on", type="timestamp"),
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
    DAG_ID,
    schedule_interval="@weekly",
    default_args=DAG_DEFAULT_ARGS,
    description=DAG_DESCRIPTION,
)

TASK_ID = "select"
TASK = RedshiftSQLOperator(
    task_id=TASK_ID,
    redshift_conn_id=CONN_ID,
    sql=SQL,
    dag=DAG,
)


@mock.patch("airflow.providers.openlineage.extractors.sql_extractor.get_table_schemas")
@mock.patch("airflow.hooks.base.BaseHook.get_connection")
def test_extract(get_connection, mock_get_table_schemas):
    table_name = f"{DB_NAME}.{DB_SCHEMA_NAME}.{DB_TABLE_NAME.name}"
    mock_get_table_schemas.return_value = (
        [
            Dataset(
                namespace="redshift://redshift-cluster-name.id.region.redshift.amazonaws.com:5439",
                name=table_name,
                facets={"schema": SCHEMA_FACET},
            )
        ],
        [],
    )

    conn = Connection(
        conn_id=CONN_ID,
        conn_type="redshift",
        host="redshift-cluster-name.id.region.redshift.amazonaws.com",
        port=5439,
        schema="food_delivery",
    )

    get_connection.return_value = conn

    expected_inputs = [
        Dataset(
            namespace="redshift://redshift-cluster-name.id.region.redshift.amazonaws.com:5439",
            name=table_name,
            facets={"schema": SCHEMA_FACET},
        ),
    ]

    task_metadata = RedshiftSQLExtractor(TASK).extract()

    assert task_metadata.inputs == expected_inputs
    assert task_metadata.outputs == []


def test_parsing_hostname():
    extractor = RedshiftSQLExtractor(TASK)
    assert extractor._get_cluster_identifier_from_hostname("1.2.3.4.5") == "1.2.3.4.5"
    assert (
        extractor._get_cluster_identifier_from_hostname(
            "redshift-cluster-name.id.region.redshift.amazonaws.com"
        )
        == "redshift-cluster-name.region"
    )


@mock.patch("airflow.hooks.base.BaseHook.get_connection")
def test_authority_with_clustername_in_host(get_connection):
    conn = Connection()
    conn.parse_from_uri(uri=CONN_URI)
    get_connection.return_value = conn
    assert RedshiftSQLExtractor(TASK)._get_authority() == "redshift-cluster-name.region:5439"


@mock.patch("airflow.hooks.base.BaseHook.get_connection")
def test_authority_with_iam(get_connection):
    conn = Connection(
        extra={
            "iam": True,
            "cluster_identifier": "redshift-cluster-name",
            "region": "region",
        }
    )
    get_connection.return_value = conn

    assert RedshiftSQLExtractor(TASK)._get_authority() == "redshift-cluster-name.region:5439"


@mock.patch("airflow.hooks.base.BaseHook.get_connection")
def test_authority_with_iam_and_implicit_region(get_connection):
    import os

    os.environ["AWS_DEFAULT_REGION"] = "region_2"
    conn = Connection(
        extra={
            "iam": True,
            "cluster_identifier": "redshift-cluster-name",
        }
    )
    get_connection.return_value = conn

    assert RedshiftSQLExtractor(TASK)._get_authority() == "redshift-cluster-name.region_2:5439"


@mock.patch("airflow.hooks.base.BaseHook.get_connection")
def test_authority_without_iam_wrong_connection(get_connection):
    conn = Connection(
        extra={
            "iam": False,
            "cluster_identifier": "redshift-cluster-name",
            "region": "region",
        }
    )
    get_connection.return_value = conn
    with pytest.raises(ValueError):
        RedshiftSQLExtractor(TASK)._get_authority()


def test_get_connection_import_returns_none_if_not_exists():
    assert get_connection("does_not_exist") is None
    assert get_connection("does_exist") is None


@pytest.fixture
def create_connection():
    conn = Connection("does_exist", conn_type="redshift")
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
