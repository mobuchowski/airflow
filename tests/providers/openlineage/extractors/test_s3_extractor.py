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

import logging
import unittest
from unittest import TestCase

from openlineage.client.run import Dataset

from airflow.models import DAG
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator, S3FileTransformOperator
from airflow.providers.openlineage.extractors.base import OperatorLineage
from airflow.providers.openlineage.extractors.s3_extractor import (
    S3CopyObjectExtractor,
    S3FileTransformExtractor,
)
from airflow.utils import timezone

log = logging.getLogger(__name__)


class TestS3CopyObjectExtractor(TestCase):
    def setUp(self):
        log.debug("TestS3CopyObjectExtractor.setup(): ")
        self.task = TestS3CopyObjectExtractor._get_copy_task()
        self.extractor = S3CopyObjectExtractor(operator=self.task)

    def test_extract(self):
        expected_return_value = OperatorLineage(
            inputs=[
                Dataset(
                    namespace="s3://source-bucket",
                    name="s3://source-bucket/path/to/source_file.csv",
                    facets={},
                )
            ],
            outputs=[
                Dataset(
                    namespace="s3://destination-bucket",
                    name="s3://destination-bucket/path/to/destination_file.csv",
                    facets={},
                )
            ],
        )
        return_value = self.extractor.extract()
        self.assertEqual(return_value, expected_return_value)

    @staticmethod
    def _get_copy_task():
        dag = DAG(dag_id="TestS3CopyObjectExtractor")
        task = S3CopyObjectOperator(
            task_id="task_id",
            source_bucket_name="source-bucket",
            source_bucket_key="path/to/source_file.csv",
            dest_bucket_name="destination-bucket",
            dest_bucket_key="path/to/destination_file.csv",
            dag=dag,
            start_date=timezone.datetime(2016, 2, 1, 0, 0, 0),
        )
        return task


class TestS3FileTransformExtractor(TestCase):
    def setUp(self):
        log.debug("TestS3FileTransformExtractor.setup(): ")
        self.task = TestS3FileTransformExtractor._get_copy_task()
        self.extractor = S3FileTransformExtractor(operator=self.task)

    def test_extract(self):
        expected_return_value = OperatorLineage(
            inputs=[
                Dataset(
                    namespace="s3://source-bucket",
                    name="s3://source-bucket/path/to/source_file.csv",
                    facets={},
                )
            ],
            outputs=[
                Dataset(
                    namespace="s3://destination-bucket",
                    name="s3://destination-bucket/path/to/destination_file.csv",
                    facets={},
                )
            ],
        )
        return_value = self.extractor.extract()
        self.assertEqual(return_value, expected_return_value)

    @staticmethod
    def _get_copy_task():
        dag = DAG(dag_id="TestS3FileTransformExtractor")
        task = S3FileTransformOperator(
            task_id="task_id",
            source_aws_conn_id="aws_default",
            dest_aws_conn_id="aws_default",
            source_s3_key="s3://source-bucket/path/to/source_file.csv",
            dest_s3_key="s3://destination-bucket/path/to/destination_file.csv",
            transform_script="cp",
            replace=True,
            dag=dag,
            start_date=timezone.datetime(2016, 2, 1, 0, 0, 0),
        )
        return task


if __name__ == "__main__":
    unittest.main()
