#
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
from __future__ import annotations

import typing

from airflow.compat.functools import cache
from airflow.utils.module_loading import import_string

if typing.TYPE_CHECKING:
    from openlineage.client.run import Dataset as OpenLineageDataset


class LineageCollector:
    """Info."""

    def __init__(self):
        self.inputs: list[OpenLineageDataset] = []
        self.outputs: list[OpenLineageDataset] = []

    def add_input(self, input: OpenLineageDataset):
        self.inputs.append(input)

    def add_output(self, output: OpenLineageDataset):
        self.outputs.append(output)

    @property
    def collected(self) -> tuple[list[OpenLineageDataset], list[OpenLineageDataset]]:
        return self.inputs, self.outputs


_collector = LineageCollector()


@cache
def does_openlineage_exist() -> bool:
    is_disabled = import_string("apache.airflow.providers.openlineage.plugin._is_disabled")
    return is_disabled and is_disabled()


def get_hook_lineage_collector() -> LineageCollector:
    return _collector
