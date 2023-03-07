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
from openlineage.client.facet import SqlJobFacet
from openlineage.common.provider.redshift_data import RedshiftDataDatasetsProvider
from openlineage.common.sql import SqlMeta, parse


class RedshiftDataExtractor(BaseExtractor):
    """RedshiftDataOperator extractor"""

    @classmethod
    def get_operator_classnames(cls) -> list[str]:
        return ["RedshiftDataOperator"]

    def extract(self) -> OperatorLineage | None:
        return None

    def extract_on_complete(self, task_instance) -> OperatorLineage | None:
        self.log.debug("extract_on_complete(%s)", str(task_instance))
        job_facets = {"sql": SqlJobFacet(query=self.operator.sql)}

        self.log.debug("Sending SQL to parser: %s", str(self.operator.sql))
        sql_meta: SqlMeta | None = parse(
            self.operator.sql, dialect=self.dialect, default_schema=self.default_schema
        )
        self.log.debug("Got meta %s", str(sql_meta))
        try:
            redshift_job_id = self._get_xcom_redshift_job_id(task_instance)
            if redshift_job_id is None:
                raise Exception("Xcom could not resolve Redshift job id. Job may have failed.")
        except Exception as e:
            self.log.error("Cannot retrieve job details from %s", e, exc_info=True)
            return OperatorLineage(
                run_facets={},
                job_facets=job_facets,
            )

        client = self.operator.hook.conn

        redshift_details = [
            "database",
            "cluster_identifier",
            "db_user",
            "secret_arn",
            "region",
        ]

        connection_details = {detail: getattr(self.operator, detail) for detail in redshift_details}

        stats = RedshiftDataDatasetsProvider(client=client, connection_details=connection_details).get_facets(
            job_id=redshift_job_id,
            inputs=sql_meta.in_tables if sql_meta else [],
            outputs=sql_meta.out_tables if sql_meta else [],
        )

        return OperatorLineage(
            inputs=[ds.to_openlineage_dataset() for ds in stats.inputs],
            outputs=[ds.to_openlineage_dataset() for ds in stats.output],
            run_facets=stats.run_facets,
            job_facets={"sql": SqlJobFacet(self.operator.sql)},
        )

    @property
    def dialect(self):
        return "redshift"

    @property
    def default_schema(self):
        # TODO: check default schema in redshift
        return "public"

    def _get_xcom_redshift_job_id(self, task_instance):
        redshift_job_id = task_instance.xcom_pull(task_ids=task_instance.task_id)

        self.log.debug("redshift job id: %s", str(redshift_job_id))
        return redshift_job_id
