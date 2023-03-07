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

# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
from unittest import TestCase

from openlineage.client.run import Dataset

from airflow.models import DAG
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.openlineage.extractors.base import OperatorLineage
from airflow.providers.openlineage.extractors.gcs_extractor import GCSToGCSExtractor
from airflow.utils import timezone

log = logging.getLogger(__name__)


class TestGCSToGCSExtractor(TestCase):
    def setUp(self):
        log.debug("TestGCSToGCSExtractor.setup(): ")
        self.task = TestGCSToGCSExtractor._get_copy_task()
        self.extractor = GCSToGCSExtractor(operator=self.task)

    def test_extract(self):
        expected_return_value = OperatorLineage(
            inputs=[
                Dataset(
                    namespace="gs://source-bucket",
                    name="gs://source-bucket/path/to/source_file.csv",
                    facets={},
                )
            ],
            outputs=[
                Dataset(
                    namespace="gs://destination-bucket",
                    name="gs://destination-bucket/path/to/destination_file.csv",
                    facets={},
                )
            ],
        )
        return_value = self.extractor.extract()
        self.assertEqual(return_value, expected_return_value)

    @staticmethod
    def _get_copy_task():
        dag = DAG(dag_id="TestGCSToGCSExtractor")
        task = GCSToGCSOperator(
            task_id="task_id",
            source_bucket="source-bucket",
            source_object="path/to/source_file.csv",
            destination_bucket="destination-bucket",
            destination_object="path/to/destination_file.csv",
            dag=dag,
            start_date=timezone.datetime(2016, 2, 1, 0, 0, 0),
        )
        return task
