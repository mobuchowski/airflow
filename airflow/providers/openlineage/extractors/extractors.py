# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import os

from airflow.providers.openlineage.extractors.base import BaseExtractor, DefaultExtractor


class Extractors:
    """
    This exposes implemented extractors, while hiding ones that require additional, unmet
    dependency. Patchers are a category of extractor that needs to hook up to operator's
    internals during DAG creation.
    """

    def __init__(self):
        # Do not expose extractors relying on external dependencies that are not installed
        self.extractors = {}
        self.default_extractor = DefaultExtractor

        # Comma-separated extractors in OPENLINEAGE_EXTRACTORS variable.
        # Extractors should implement BaseExtractor
        from airflow.providers.openlineage.utils import import_from_string
        
        env_extractors = os.getenv("OPENLINEAGE_EXTRACTORS")
        if env_extractors is not None:
            for extractor in env_extractors.split(';'):
                extractor = import_from_string(extractor.strip())
                for operator_class in extractor.get_operator_classnames():
                    self.extractors[operator_class] = extractor

        # Previous way of adding extractors
        # Adding operator: extractor pairs registered via environmental variable in pattern
        # OPENLINEAGE_EXTRACTOR_<operator>=<path.to.ExtractorClass>
        # The value in extractor map is extractor class type - it needs to be instantiated.
        # We import the module provided and get type using importlib then.
        for key, value in os.environ.items():
            if key.startswith("OPENLINEAGE_EXTRACTOR_"):
                extractor = import_from_string(value)
                for operator_class in extractor.get_operator_classnames():
                    self.extractors[operator_class] = extractor

    def add_extractor(self, operator: str, extractor: type):
        self.extractors[operator] = extractor

    def get_extractor_class(self, clazz: type) -> type[BaseExtractor] | None:
        name = clazz.__name__
        if name in self.extractors:
            return self.extractors[name]

        def method_exists(method_name):
            method = getattr(clazz, method_name, None)
            if method:
                return callable(method)

        if method_exists("get_openlineage_facets_on_start") or method_exists(
            "get_openlineage_facets_on_complete"
        ):
            return self.default_extractor
        return None
