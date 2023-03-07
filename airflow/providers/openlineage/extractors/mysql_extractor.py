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

from airflow.providers.openlineage.extractors.sql_extractor import SqlExtractor
from airflow.utils.module_loading import import_string


class MySqlExtractor(SqlExtractor):
    """MySQL extractor"""

    _information_schema_columns = [
        "table_schema",
        "table_name",
        "column_name",
        "ordinal_position",
        "column_type",
    ]

    @property
    def dialect(self):
        return "mysql"

    @property
    def default_schema(self):
        return None

    @classmethod
    def get_operator_classnames(cls) -> list[str]:
        return ["MySqlOperator"]

    def _get_scheme(self) -> str:
        return "mysql"

    def _get_database(self) -> None:
        # MySQL does not have database concept.
        return None

    def _get_hook(self):
        with suppress(ImportError):
            MySqlHook = import_string("airflow.providers.mysql.hooks.mysql.MySqlHook")
            return MySqlHook(mysql_conn_id=self.operator.mysql_conn_id, schema=self.operator.database)

    @staticmethod
    def _normalize_name(name: str) -> str:
        return name.upper()
