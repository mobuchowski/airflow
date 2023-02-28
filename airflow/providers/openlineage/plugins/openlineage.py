# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import os

from airflow.plugins_manager import AirflowPlugin
from airflow.providers.openlineage.plugins.macros import lineage_parent_id, lineage_run_id


def _is_disabled():
    return os.getenv("OPENLINEAGE_DISABLED", None) in [True, 'true', "True"]


if _is_disabled():  # type: ignore
    # Provide empty plugin when OL is disabled
    class OpenLineageProviderPlugin(AirflowPlugin):
        """OpenLineage plugin that provides macros only"""

        name = "OpenLineageProviderPlugin"
        macros = [lineage_run_id, lineage_parent_id]
else:
    from airflow.providers.openlineage.plugins.listener import OpenLineageListener

    # Provide entrypoint airflow plugin that registers listener module
    class OpenLineageProviderPlugin(AirflowPlugin):     # type: ignore
        """OpenLineage plugin that provides listener module and macros"""

        name = "OpenLineageProviderPlugin"
        listeners = [OpenLineageListener()]
        macros = [lineage_run_id, lineage_parent_id]
