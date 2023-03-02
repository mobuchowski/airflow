# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from airflow.listeners.listener import get_listener_manager
from airflow.models import BaseOperator
from airflow.providers.openlineage.plugins.listener import OpenLineageListener


class TemplateOperator(BaseOperator):
    template_fields = ["df"]

    def __init__(self, df, *args, **kwargs):
        self.df = df
        super().__init__(*args, **kwargs)

    def execute(self, _):
        return self.df


def render_param():
    return "test_param"


@patch("airflow.models.TaskInstance.xcom_push")
@patch("airflow.models.BaseOperator.render_template")
def test_listener_does_not_change_task_instance(render_mock, xcom_push_mock, dag_maker):
    # register OL listener
    ol_listener = OpenLineageListener()
    get_listener_manager().add_listener(ol_listener)
    ol_listener.executor = ThreadPoolExecutor(thread_name_prefix="test_openlineage_")

    render_mock.return_value = render_param()

    with dag_maker(user_defined_macros={"render_df": render_param},
        params={"df": render_param()}) as dag:
        TemplateOperator(
            task_id="template_op", do_xcom_push=True, df=dag.param("df")
        )
        (ti,) = dag_maker.create_dagrun().task_instances

    # Use ti.run() to capture state change to RUNNING
    ti.run()

    # check if task returns the same param
    assert xcom_push_mock.call_args[1]["value"] == render_param()

    # clear listeners
    get_listener_manager().clear()
    ol_listener.executor.shutdown()

    # check if render_template method always gets the same unrendered field
    assert not isinstance(render_mock.call_args[0][0], str)
