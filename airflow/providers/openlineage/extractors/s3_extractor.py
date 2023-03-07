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
from urllib.parse import urlparse

from airflow.providers.openlineage.extractors.base import BaseExtractor, OperatorLineage
from openlineage.client.run import Dataset

log = logging.getLogger(__name__)


class S3CopyObjectExtractor(BaseExtractor):
    """S3CopyObjectOperator extractor"""

    @classmethod
    def get_operator_classnames(cls) -> list[str]:
        return ["S3CopyObjectOperator"]

    def extract(self) -> OperatorLineage | None:
        input_object = Dataset(
            namespace=f"s3://{self.operator.source_bucket_name}",
            name=f"s3://{self.operator.source_bucket_name}/{self.operator.source_bucket_key}",
            facets={},
        )

        output_object = Dataset(
            namespace=f"s3://{self.operator.dest_bucket_name}",
            name=f"s3://{self.operator.dest_bucket_name}/{self.operator.dest_bucket_key}",
            facets={},
        )

        return OperatorLineage(
            inputs=[input_object],
            outputs=[output_object],
        )

    def extract_on_complete(self, task_instance) -> OperatorLineage | None:
        pass


class S3FileTransformExtractor(BaseExtractor):
    """S3FileTransformOperator extractor"""

    @classmethod
    def get_operator_classnames(cls) -> list[str]:
        return ["S3FileTransformOperator"]

    def extract(self) -> OperatorLineage | None:
        input_object = Dataset(
            namespace=f"s3://{urlparse(self.operator.source_s3_key).netloc}",
            name=self.operator.source_s3_key,
            facets={},
        )

        output_object = Dataset(
            namespace=f"s3://{urlparse(self.operator.dest_s3_key).netloc}",
            name=self.operator.dest_s3_key,
            facets={},
        )

        return OperatorLineage(
            inputs=[input_object],
            outputs=[output_object],
        )

    def extract_on_complete(self, task_instance) -> OperatorLineage | None:
        pass
