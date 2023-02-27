# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import attr

from airflow.providers.openlineage import version as OPENLINEAGE_AIRFLOW_VERSION
from airflow.version import version as AIRFLOW_VERSION
from openlineage.client.facet import BaseFacet
from openlineage.client.utils import RedactMixin


@attr.s
class AirflowVersionRunFacet(BaseFacet):
    """Run facet containing task and DAG info"""

    operator: str = attr.ib()
    taskInfo: dict[str, object] = attr.ib()
    airflowVersion: str = attr.ib()
    openlineageAirflowVersion: str = attr.ib()


    _additional_skip_redact: list[str] = [
        "operator",
        "airflowVersion",
        "openlineageAirflowVersion",
    ]

    @classmethod
    def from_dagrun_and_task(cls, dagrun, task):
        # task.__dict__ may contain values uncastable to str
        from airflow.providers.openlineage.utils import get_operator_class, to_json_encodable

        task_info = to_json_encodable(task)
        task_info["dag_run"] = to_json_encodable(dagrun)

        return cls(
            operator=f"{get_operator_class(task).__module__}.{get_operator_class(task).__name__}",
            taskInfo=task_info,
            airflowVersion=AIRFLOW_VERSION,
            openlineageAirflowVersion=OPENLINEAGE_AIRFLOW_VERSION,
        )


@attr.s
class AirflowRunArgsRunFacet(BaseFacet):
    """Run facet pointing if DAG was triggered manually"""
    
    externalTrigger: bool = attr.ib(default=False)

    _additional_skip_redact: list[str] = ["externalTrigger"]


@attr.s
class AirflowMappedTaskRunFacet(BaseFacet):
    """Run facet containing information about mapped tasks"""

    mapIndex: int = attr.ib()
    operatorClass: str = attr.ib()

    _additional_skip_redact: list[str] = ["operatorClass"]

    @classmethod
    def from_task_instance(cls, task_instance):
        task = task_instance.task
        from airflow.providers.openlineage.utils import get_operator_class

        return cls(
            task_instance.map_index,
            f"{get_operator_class(task).__module__}.{get_operator_class(task).__name__}",
        )


@attr.s
class AirflowRunFacet(BaseFacet):
    """Composite Airflow run facet."""

    dag: dict = attr.ib()
    dagRun: dict = attr.ib()
    task: dict = attr.ib()
    taskInstance: dict = attr.ib()
    taskUuid: str = attr.ib()


@attr.s
class UnknownOperatorInstance(RedactMixin):
    """
    Describes an unknown operator - specifies the (class) name of the operator
    and its properties
    """

    name: str = attr.ib()
    properties: dict[str, object] = attr.ib()
    type: str = attr.ib(default="operator")

    _skip_redact: list[str] = ["name", "type"]


@attr.s
class UnknownOperatorAttributeRunFacet(BaseFacet):
    """RunFacet that describes unknown operators in an Airflow DAG"""

    unknownItems: list[UnknownOperatorInstance] = attr.ib()
