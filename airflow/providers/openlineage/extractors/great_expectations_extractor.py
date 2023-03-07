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

from airflow.providers.openlineage.extractors.base import BaseExtractor, OperatorLineage

# Great Expectations is optional dependency.
try:
    from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator

    _has_great_expectations = True
except Exception:
    # Create placeholder for GreatExpectationsOperator
    GreatExpectationsOperator = None
    _has_great_expectations = False


class GreatExpectationsExtractorImpl(BaseExtractor):
    """
    Great Expectations extractor extracts validation data from CheckpointResult object and
    parses it via ExpectationsParsers. Results are used to prepare data quality facet.
    """

    def __init__(self, operator):
        super().__init__(operator)
        self.result = None

    @classmethod
    def get_operator_classnames(cls) -> list[str]:
        return [GreatExpectationsOperator.__name__] if GreatExpectationsOperator else []

    def extract(self) -> OperatorLineage | None:
        return None

    def extract_on_complete(self, task_instance) -> OperatorLineage | None:
        return None


if _has_great_expectations:
    GreatExpectationsExtractor = GreatExpectationsExtractorImpl
else:

    class GreatExpectationsExtractor:  # type: ignore
        """Great Expectations extractor"""

        def __init__(self):
            raise RuntimeError("Great Expectations provider not found")

        @classmethod
        def get_operator_classnames(cls) -> list[str]:
            return []
