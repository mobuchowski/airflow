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

from airflow.providers.openlineage.extractors.base import BaseExtractor, OperatorLineage
from airflow.providers.openlineage.plugins.facets import (
    UnknownOperatorAttributeRunFacet,
    UnknownOperatorInstance,
)
from openlineage.client.facet import SourceCodeJobFacet


class BashExtractor(BaseExtractor):
    """
    This extractor provides visibility on what bash task does by extracting
    executed bash command and putting it into SourceCodeJobFacet. It does not extract
    datasets.
    """

    @classmethod
    def get_operator_classnames(cls) -> list[str]:
        return ["BashOperator"]

    def extract(self) -> OperatorLineage | None:
        collect_source = os.environ.get("OPENLINEAGE_AIRFLOW_DISABLE_SOURCE_CODE", "True").lower() not in (
            "true",
            "1",
            "t",
        )

        job_facet: dict = {}
        if collect_source:
            job_facet = {
                "sourceCode": SourceCodeJobFacet(
                    "bash",
                    # We're on worker and should have access to DAG files
                    self.operator.bash_command,
                )
            }

        return OperatorLineage(
            job_facets=job_facet,
            run_facets={
                # The BashOperator is recorded as an "unknownSource" even though we have an
                # extractor, as the <i>data lineage</i> cannot be determined from the operator
                # directly.
                "unknownSourceAttribute": UnknownOperatorAttributeRunFacet(
                    unknownItems=[
                        UnknownOperatorInstance(
                            name="BashOperator",
                            properties={attr: value for attr, value in self.operator.__dict__.items()},
                        )
                    ]
                )
            },
        )
