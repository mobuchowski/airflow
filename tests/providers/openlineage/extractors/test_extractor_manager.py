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

import os
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from airflow.hooks.base import BaseHook
from airflow.models import Connection
from airflow.models.baseoperator import BaseOperator
from airflow.providers.openlineage.extractors.base import BaseExtractor, OperatorLineage
from airflow.providers.openlineage.extractors.manager import ExtractorManager
from airflow.providers.openlineage.extractors.postgres_extractor import PostgresExtractor


class FakeOperator(BaseOperator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.executed = False

    def execute(self, context: Any):
        self.executed = True


class FakeExtractor(BaseExtractor):
    @classmethod
    def get_operator_classnames(cls) -> list[str]:
        return ["FakeOperator"]

    def extract(self) -> OperatorLineage | None:
        return OperatorLineage(job_facets={"fake": {"executed": self.operator.executed}})

    def extract_on_complete(self, task_instance) -> OperatorLineage | None:
        from openlineage.client.run import Dataset

        return OperatorLineage(
            job_facets={"fake": {"executed": self.operator.executed}},
            inputs=[Dataset(namespace="example", name="ip_table")],
            outputs=[Dataset(namespace="example", name="op_table")],
        )


class AnotherFakeExtractor(BaseExtractor):
    def extract(self) -> OperatorLineage | None:
        return None

    @classmethod
    def get_operator_classnames(cls) -> list[str]:
        return ["AnotherFakeOperator"]


def test_fake_extractor_extracts():
    dagrun = MagicMock()
    task = FakeOperator(task_id="task")

    manager = ExtractorManager()
    manager.add_extractor(FakeOperator.__name__, FakeExtractor)
    metadata = manager.extract_metadata(dagrun, task)

    assert len(metadata.job_facets) == 1
    assert metadata.job_facets["fake"]["executed"] is False


def test_adding_extractors_to_manager():
    manager = ExtractorManager()
    count = len(manager.extractors)
    manager.add_extractor("test", PostgresExtractor)
    assert len(manager.extractors) == count + 1


def test_extracting_inlets_and_outlets():
    from openlineage.client.run import Dataset

    from airflow.lineage.entities import Table

    metadata = OperatorLineage(job_facets={})
    inlets = [Dataset(namespace="c1", name="d1.t0", facets={}), Table(database="d1", cluster="c1", name="t1")]
    outlets = [Table(database="d1", cluster="c1", name="t2")]

    manager = ExtractorManager()
    manager.extract_inlets_and_outlets(metadata, inlets, outlets)

    assert len(metadata.inputs) == 2 and len(metadata.outputs) == 1
    assert isinstance(metadata.inputs[0], Dataset)
    assert isinstance(metadata.inputs[1], Dataset)
    assert isinstance(metadata.outputs[0], Dataset)


def test_extraction_from_inlets_and_outlets_without_extractor():
    from openlineage.client.run import Dataset

    from airflow.lineage.entities import Table

    dagrun = MagicMock()

    task = FakeOperator(
        task_id="task",
        inlets=[
            Dataset(namespace="c1", name="d1.t0", facets={}),
            Table(database="d1", cluster="c1", name="t1"),
        ],
        outlets=[Table(database="d1", cluster="c1", name="t2")],
    )

    manager = ExtractorManager()

    metadata = manager.extract_metadata(dagrun, task)
    assert len(metadata.inputs) == 2 and len(metadata.outputs) == 1
    assert isinstance(metadata.inputs[0], Dataset)
    assert isinstance(metadata.inputs[1], Dataset)
    assert isinstance(metadata.outputs[0], Dataset)


def test_extraction_from_inlets_and_outlets_ignores_unhandled_types():
    from openlineage.client.run import Dataset

    from airflow.lineage.entities import File, Table

    dagrun = MagicMock()

    task = FakeOperator(
        task_id="task",
        inlets=[
            Dataset(namespace="c1", name="d1.t0", facets={}),
            File(url="http://test"),
            Table(database="d1", cluster="c1", name="t1"),
        ],
        outlets=[Table(database="d1", cluster="c1", name="t2"), File(url="http://test")],
    )

    manager = ExtractorManager()

    metadata = manager.extract_metadata(dagrun, task)
    # The File objects from inlets and outlets should not be converted
    assert len(metadata.inputs) == 2 and len(metadata.outputs) == 1


def test_fake_extractor_extracts_from_inlets_and_outlets():
    from openlineage.client.run import Dataset

    from airflow.lineage.entities import Table

    dagrun = MagicMock()

    task = FakeOperator(
        task_id="task",
        inlets=[
            Dataset(namespace="c1", name="d1.t0", facets={}),
            Table(database="d1", cluster="c1", name="t1"),
        ],
        outlets=[
            Table(database="d1", cluster="c1", name="t2"),
            Dataset(namespace="c1", name="d1.t3", facets={}),
        ],
    )

    manager = ExtractorManager()
    manager.add_extractor(FakeOperator.__name__, FakeExtractor)

    metadata = manager.extract_metadata(dagrun, task)
    assert len(metadata.inputs) == 2 and len(metadata.outputs) == 2
    assert isinstance(metadata.inputs[0], Dataset)
    assert isinstance(metadata.inputs[1], Dataset)
    assert isinstance(metadata.outputs[0], Dataset)
    assert isinstance(metadata.outputs[1], Dataset)
    assert metadata.inputs[0].name == "d1.t0"
    assert metadata.inputs[1].name == "d1.t1"
    assert metadata.outputs[0].name == "d1.t2"
    assert metadata.outputs[1].name == "d1.t3"


def test_fake_extractor_extracts_and_discards_inlets_and_outlets():
    from openlineage.client.run import Dataset

    from airflow.lineage.entities import Table

    dagrun = MagicMock()

    task = FakeOperator(
        task_id="task",
        inlets=[
            Dataset(namespace="c1", name="d1.t0", facets={}),
            Table(database="d1", cluster="c1", name="t1"),
        ],
        outlets=[Table(database="d1", cluster="c1", name="t2")],
    )

    manager = ExtractorManager()
    manager.add_extractor(FakeOperator.__name__, FakeExtractor)

    metadata = manager.extract_metadata(dagrun, task, complete=True)
    assert len(metadata.inputs) == 1 and len(metadata.outputs) == 1
    assert isinstance(metadata.inputs[0], Dataset)
    assert isinstance(metadata.outputs[0], Dataset)
    assert metadata.inputs[0].name == "ip_table"
    assert metadata.outputs[0].name == "op_table"


def test_basic_extractor():
    class PostgresOperator(BaseOperator):
        pass

    assert ExtractorManager().get_extractor_class(PostgresOperator(task_id="a"))


def test_env_add_extractor():
    extractor_list_len = len(ExtractorManager().extractors)
    with patch.dict(
        os.environ,
        {
            "OPENLINEAGE_EXTRACTORS": "tests.providers.openlineage.extractors."
            "test_extractor_manager.FakeExtractor"
        },
    ):
        assert len(ExtractorManager().extractors) == extractor_list_len + 1


def test_env_multiple_extractors():
    extractor_list_len = len(ExtractorManager().extractors)
    with patch.dict(
        os.environ,
        {
            "OPENLINEAGE_EXTRACTORS": "tests.providers.openlineage.extractors."
            "test_extractor_manager.FakeExtractor;\n"
            "tests.providers.openlineage.extractors.test_extractor_manager.AnotherFakeExtractor"
        },
    ):
        assert len(ExtractorManager().extractors) == extractor_list_len + 2


def test_adding_extractors():
    extractors = ExtractorManager()
    count = len(extractors.extractors)
    extractors.add_extractor("test", PostgresExtractor)
    assert len(extractors.extractors) == count + 1


@patch.object(
    BaseHook, "get_connection", return_value=Connection(conn_id="postgres_default", conn_type="postgres")
)
def test_instantiate_abstract_extractors(mock_hook):
    class SQLCheckOperator:
        conn_id = "postgres_default"

    manager = ExtractorManager()
    manager.instantiate_abstract_extractors(task=SQLCheckOperator())
    sql_check_extractor = manager.extractors["SQLCheckOperator"]("SQLCheckOperator")
    assert sql_check_extractor._get_scheme() == "postgres"


@patch.object(
    BaseHook, "get_connection", return_value=Connection(conn_id="postgres_default", conn_type="postgres")
)
def test_instantiate_abstract_extractors_sql_execute(mock_hook):
    class SQLExecuteQueryOperator:
        conn_id = "postgres_default"

    manager = ExtractorManager()
    manager.instantiate_abstract_extractors(task=SQLExecuteQueryOperator())
    sql_check_extractor = manager.extractors["SQLExecuteQueryOperator"]("SQLExecuteQueryOperator")
    assert sql_check_extractor._get_scheme() == "postgres"


@patch("airflow.models.connection.Connection")
@patch.object(
    BaseHook,
    "get_connection",
    return_value=Connection(conn_id="notimplemented", conn_type="notimplementeddb"),
)
def test_instantiate_abstract_extractors_value_error(mock_hook, mock_conn):
    class SQLCheckOperator:
        conn_id = "notimplementeddb"

    with pytest.raises(ValueError):
        manager = ExtractorManager()
        manager.instantiate_abstract_extractors(task=SQLCheckOperator())
