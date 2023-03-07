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
from urllib.parse import urlparse

from airflow.providers.openlineage.extractors.sql_extractor import SqlExtractor
from airflow.utils.module_loading import import_string


class PostgresExtractor(SqlExtractor):
    """Postgres extractor"""

    _information_schema_columns = [
        "table_schema",
        "table_name",
        "column_name",
        "ordinal_position",
        "udt_name",
    ]
    _is_information_schema_cross_db = False

    # cluster-identifier
    @property
    def dialect(self):
        return "postgres"

    @classmethod
    def get_operator_classnames(cls) -> list[str]:
        return ["PostgresOperator"]

    def _get_scheme(self):
        return "postgres"

    def _get_database(self) -> str | None:
        if not self.conn:
            return None
        if self.conn.schema:
            return self.conn.schema
        else:
            parsed = urlparse(self.conn.get_uri())
            return f"{parsed.path}"

    def _get_hook(self):
        with suppress(ImportError):
            PostgresHook = import_string("airflow.providers.postgres.hooks.postgres.PostgresHook")
        return PostgresHook(postgres_conn_id=self.operator.postgres_conn_id, schema=self.operator.database)
