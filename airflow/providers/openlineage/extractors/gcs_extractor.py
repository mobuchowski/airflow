# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging

from airflow.providers.openlineage.extractors.base import BaseExtractor, OperatorLineage
from openlineage.client.run import Dataset

log = logging.getLogger(__name__)


class GCSToGCSExtractor(BaseExtractor):
    """GCSToGCSOperator extractor"""

    @classmethod
    def get_operator_classnames(cls) -> list[str]:
        return ['GCSToGCSOperator']

    def extract(self) -> OperatorLineage | None:
        if self.operator.source_object:
            input_objects = [Dataset(
                namespace=f"gs://{self.operator.source_bucket}",
                name=f"gs://{self.operator.source_bucket}/{self.operator.source_object}",
                facets={},
            )]
        else:
            input_objects = [Dataset(
                namespace=f"gs://{self.operator.source_bucket}",
                name=f"gs://{self.operator.source_bucket}/{source_object}",
                facets={},
            ) for source_object in self.operator.source_objects]

        output_object = Dataset(
            namespace=f"gs://{self.operator.destination_bucket}",
            name=f"gs://{self.operator.destination_bucket}/{self.operator.destination_object}",
            facets={},
        )

        return OperatorLineage(
            inputs=input_objects,
            outputs=[output_object],
        )

    def extract_on_complete(self, task_instance) -> OperatorLineage | None:
        pass
