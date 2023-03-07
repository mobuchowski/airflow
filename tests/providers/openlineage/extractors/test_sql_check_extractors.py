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

from airflow.providers.openlineage.extractors.postgres_extractor import PostgresExtractor
from airflow.providers.openlineage.extractors.sql_check_extractors import get_check_extractors

COLUMN_CHECK_MAPPING = {
    "X": {
        "null_check": {"pass_value": 0, "tolerance": 0.0, "result": 0, "success": True},
        "distinct_check": {"pass_value": 5, "tolerance": 0.0, "result": 6, "success": False},
    }
}
TABLE_CHECK_MAPPING = {"row_count_check": {"pass_value": 9, "result": 9, "success": True}}

(
    SqlCheckExtractor,
    SqlValueCheckExtractor,
    SqlThresholdCheckExtractor,
    SqlIntervalCheckExtractor,
    SqlColumnCheckExtractor,
    SqlTableCheckExtractor,
) = get_check_extractors(PostgresExtractor)


class SQLTableCheckOperator:
    checks = TABLE_CHECK_MAPPING


class SQLColumnCheckOperator:
    column_mapping = COLUMN_CHECK_MAPPING


def test_get_table_input_facets():
    extractor = SqlTableCheckExtractor(SQLTableCheckOperator())
    facets = extractor._get_input_facets()
    data_quality_facet = facets["dataQuality"]
    assertions_facet = facets["dataQualityAssertions"]
    assert data_quality_facet.rowCount == 9
    assert data_quality_facet.bytes is None
    assert assertions_facet.assertions[0].assertion == "row_count_check"
    assert assertions_facet.assertions[0].success


def test_get_column_input_facets():
    extractor = SqlColumnCheckExtractor(SQLColumnCheckOperator())
    facets = extractor._get_input_facets()
    data_quality_facet = facets["dataQuality"]
    assertions_facet = facets["dataQualityAssertions"]
    assert data_quality_facet.columnMetrics.get("X").nullCount == 0
    assert data_quality_facet.columnMetrics.get("X").distinctCount == 6
    assert data_quality_facet.rowCount is None
    assert data_quality_facet.bytes is None
    assert assertions_facet.assertions[0].assertion == "null_check"
    assert assertions_facet.assertions[0].success
    assert assertions_facet.assertions[0].column == "X"
    assert assertions_facet.assertions[1].assertion == "distinct_check"
    assert not assertions_facet.assertions[1].success
    assert assertions_facet.assertions[1].column == "X"
