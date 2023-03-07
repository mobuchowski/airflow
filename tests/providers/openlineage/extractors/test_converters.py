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


def test_table_to_dataset_conversion():
    from airflow.lineage.entities import Table
    from airflow.providers.openlineage.extractors import ExtractorManager

    t = Table(
        database="db",
        cluster="c",
        name="table1",
    )

    d = ExtractorManager.convert_to_ol_dataset(t)

    assert d.namespace == "c"
    assert d.name == "db.table1"


def test_dataset_to_dataset_conversion():
    from openlineage.client.run import Dataset

    from airflow.providers.openlineage.extractors import ExtractorManager

    t = Dataset(
        namespace="c",
        name="db.table1",
        facets={},
    )

    d = ExtractorManager.convert_to_ol_dataset(t)

    assert d.namespace == "c"
    assert d.name == "db.table1"
