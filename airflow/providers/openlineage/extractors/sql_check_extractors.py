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

from collections import defaultdict
from typing import Any

from airflow.providers.openlineage.extractors.base import OperatorLineage
from openlineage.client.facet import (
    Assertion,
    BaseFacet,
    ColumnMetric,
    DataQualityAssertionsDatasetFacet,
    DataQualityMetricsInputDatasetFacet,
)


def get_check_extractors(super_):
    class BaseSqlCheckExtractor(super_):
        def __init__(self, operator):
            super().__init__(operator)

        def extract(self) -> OperatorLineage:
            return super().extract()

        def extract_on_complete(self, task_instance) -> OperatorLineage | None:
            from airflow.providers.openlineage.extractors.bigquery_extractor import BigQueryExtractor

            if issubclass(BigQueryExtractor, BaseSqlCheckExtractor):
                return super().extract_on_complete(task_instance)
            return super().extract()

    class SqlCheckExtractor(BaseSqlCheckExtractor):
        def __init__(self, operator):
            super().__init__(operator)

        @classmethod
        def get_operator_classnames(cls) -> list[str]:
            return ["SQLCheckOperator"]

        def _get_input_facets(self) -> dict[str, BaseFacet]:
            return {}

    class SqlValueCheckExtractor(BaseSqlCheckExtractor):
        def __init__(self, operator):
            super().__init__(operator)

        @classmethod
        def get_operator_classnames(cls) -> list[str]:
            return ["SQLValueCheckOperator"]

        def _get_input_facets(self) -> dict[str, BaseFacet]:
            return {}

    class SqlThresholdCheckExtractor(BaseSqlCheckExtractor):
        def __init__(self, operator):
            super().__init__(operator)

        @classmethod
        def get_operator_classnames(cls) -> list[str]:
            return ["SQLThresholdCheckOperator"]

        def _get_input_facets(self) -> dict[str, BaseFacet]:
            return {}

    class SqlIntervalCheckExtractor(BaseSqlCheckExtractor):
        def __init__(self, operator):
            super().__init__(operator)

        @classmethod
        def get_operator_classnames(cls) -> list[str]:
            return ["SQLIntervalCheckOperator"]

        def _get_input_facets(self) -> dict[str, BaseFacet]:
            return {}

    class SqlColumnCheckExtractor(BaseSqlCheckExtractor):
        def __init__(self, operator):
            super().__init__(operator)

        @classmethod
        def get_operator_classnames(cls) -> list[str]:
            return ["SQLColumnCheckOperator", "BigQueryColumnCheckOperator"]

        def _get_input_facets(self) -> dict[str, BaseFacet]:
            """
            Function should expect the column_mapping to take the following form:
            {
                'col_name': {
                    'null_check': {
                        'pass_value': 0,
                        'result': 0,
                        'success': True
                    },
                    'min': {
                        'pass_value': 5,
                        'tolerance': 0.2,
                        'result': 1,
                        'success': False
                    }
                }
            }
            """

            def map_facet_name(check_name) -> str:
                if "null" in check_name:
                    return "nullCount"
                elif "distinct" in check_name:
                    return "distinctCount"
                elif "sum" in check_name:
                    return "sum"
                elif "count" in check_name:
                    return "count"
                elif "min" in check_name:
                    return "min"
                elif "max" in check_name:
                    return "max"
                elif "quantiles" in check_name:
                    return "quantiles"
                return ""

            facet_data: dict[str, Any] = {"columnMetrics": defaultdict(dict)}
            assertion_data: dict[str, list[Assertion]] = {"assertions": []}
            for col_name, checks in self.operator.column_mapping.items():
                col_name = col_name.upper() if self._is_uppercase_names else col_name
                for check, check_values in checks.items():
                    facet_key = map_facet_name(check)
                    facet_data["columnMetrics"][col_name][facet_key] = check_values.get("result")

                    assertion_data["assertions"].append(
                        Assertion(assertion=check, success=check_values.get("success"), column=col_name)
                    )
                facet_data["columnMetrics"][col_name] = ColumnMetric(**facet_data["columnMetrics"][col_name])

            data_quality_facet = DataQualityMetricsInputDatasetFacet(**facet_data)
            data_quality_assertions_facet = DataQualityAssertionsDatasetFacet(**assertion_data)

            return {
                "dataQuality": data_quality_facet,
                "dataQualityMetrics": data_quality_facet,
                "dataQualityAssertions": data_quality_assertions_facet,
            }

    class SqlTableCheckExtractor(BaseSqlCheckExtractor):
        def __init__(self, operator):
            super().__init__(operator)

        @classmethod
        def get_operator_classnames(cls) -> list[str]:
            return ["SQLTableCheckOperator", "BigQueryTableCheckOperator"]

        def _get_input_facets(self) -> dict[str, BaseFacet]:
            """
            Function should expect to take the checks in the following form:
            {
                'row_count_check': {
                    'pass_value': 100,
                    'tolerance': .05,
                    'result': 101,
                    'success': True
                }
            }
            """
            facet_data = {}
            assertion_data: dict[str, list[Assertion]] = {"assertions": []}
            for check, check_values in self.operator.checks.items():
                assertion_data["assertions"].append(
                    Assertion(
                        assertion=check,
                        success=check_values.get("success"),
                    )
                )
            facet_data["rowCount"] = self.operator.checks.get("row_count_check", {}).get("result", None)
            facet_data["bytes"] = self.operator.checks.get("bytes", {}).get("result", None)

            data_quality_facet = DataQualityMetricsInputDatasetFacet(**facet_data)
            data_quality_assertions_facet = DataQualityAssertionsDatasetFacet(**assertion_data)

            return {
                "dataQuality": data_quality_facet,
                "dataQualityMetrics": data_quality_facet,
                "dataQualityAssertions": data_quality_assertions_facet,
            }

    return [
        SqlCheckExtractor,
        SqlValueCheckExtractor,
        SqlThresholdCheckExtractor,
        SqlIntervalCheckExtractor,
        SqlColumnCheckExtractor,
        SqlTableCheckExtractor,
    ]
