# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
from concurrent.futures import Executor, ThreadPoolExecutor
from typing import TYPE_CHECKING

from airflow.listeners import hookimpl
from airflow.providers.openlineage.extractors import ExtractorManager
from airflow.providers.openlineage.plugins.adapter import OpenLineageAdapter
from airflow.providers.openlineage.utils import (
    get_airflow_run_facet,
    get_custom_facets,
    get_job_name,
    get_task_location,
    print_exception,
)

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.models import DagRun, TaskInstance


class OpenLineageListener:
    """
    OpenLineage listener
    Sends events on task instance and dag run starts, completes and failures.
    """

    def __init__(self):
        self.log = logging.getLogger(__name__)
        self.executor: Executor | None = None  # type: ignore
        self.extractor_manager = ExtractorManager()
        self.adapter = OpenLineageAdapter()

    @hookimpl
    def on_task_instance_running(
        self,
        previous_state,  # This will always be QUEUED
        task_instance: TaskInstance,
        session: Session
    ):
        if not hasattr(task_instance, 'task'):
            self.log.warning(
                f"No task set for TI object task_id: {task_instance.task_id} - dag_id: {task_instance.dag_id} - run_id {task_instance.run_id}")  # noqa
            return

        self.log.debug("OpenLineage listener got notification about task instance start")
        dagrun = task_instance.dag_run
        task = task_instance.task
        dag = task.dag

        @print_exception
        def on_running():
            # that's a workaround to detect task running from deferred state
            # we return here because Airflow 2.3 needs task from deferred state
            if task_instance.next_method is not None:
                return
            parent_run_id = self.adapter.build_dag_run_id(dag.dag_id, dagrun.run_id)

            task_uuid = self.adapter.build_task_instance_run_id(
                task.task_id, task_instance.execution_date, task_instance.try_number
            )

            task_metadata = self.extractor_manager.extract_metadata(dagrun, task)

            self.adapter.start_task(
                run_id=task_uuid,
                job_name=get_job_name(task),
                job_description=dag.description,
                event_time=task_instance.start_date.isoformat(),
                parent_job_name=dag.dag_id,
                parent_run_id=parent_run_id,
                code_location=get_task_location(task),
                nominal_start_time=dagrun.data_interval_start.isoformat(),
                nominal_end_time=dagrun.data_interval_end.isoformat(),
                owners=dag.owner.split(", "),
                task=task_metadata,
                run_facets={
                    **task_metadata.run_facets,
                    **get_custom_facets(
                        dagrun, task, dagrun.external_trigger, task_instance
                    ),
                    **get_airflow_run_facet(dagrun, dag, task_instance, task, task_uuid)
                }
            )

        self.executor.submit(on_running)

    @hookimpl
    def on_task_instance_success(self, previous_state, task_instance: TaskInstance, session):
        self.log.debug("OpenLineage listener got notification about task instance success")

        dagrun = task_instance.dag_run
        task = task_instance.task

        task_uuid = OpenLineageAdapter.build_task_instance_run_id(
            task.task_id, task_instance.execution_date, task_instance.try_number - 1
        )

        @print_exception
        def on_success():
            task_metadata = self.extractor_manager.extract_metadata(
                dagrun, task, complete=True, task_instance=task_instance
            )
            self.adapter.complete_task(
                run_id=task_uuid,
                job_name=get_job_name(task),
                end_time=task_instance.end_date.isoformat(),
                task=task_metadata,
            )

        self.executor.submit(on_success)

    @hookimpl
    def on_task_instance_failed(self, previous_state, task_instance: TaskInstance, session):
        self.log.debug("OpenLineage listener got notification about task instance failure")

        dagrun = task_instance.dag_run
        task = task_instance.task

        task_uuid = OpenLineageAdapter.build_task_instance_run_id(
            task.task_id, task_instance.execution_date, task_instance.try_number - 1
        )

        @print_exception
        def on_failure():
            task_metadata = self.extractor_manager.extract_metadata(
                dagrun, task, complete=True, task_instance=task_instance
            )

            self.adapter.fail_task(
                run_id=task_uuid,
                job_name=get_job_name(task),
                end_time=task_instance.end_date.isoformat(),
                task=task_metadata,
            )

        self.executor.submit(on_failure)

    @hookimpl
    def on_starting(self, component):
        self.log.debug(f"on_starting: {component.__class__.__name__}")
        self.executor = ThreadPoolExecutor(max_workers=8, thread_name_prefix="openlineage_")

    @hookimpl
    def before_stopping(self, component):
        self.log.debug(f"before_stopping: {component.__class__.__name__}")
        self.executor.shutdown(wait=True)

    @hookimpl
    def on_dag_run_running(self, dag_run: DagRun, msg: str):
        if not self.executor:
            self.log.error("Executor have not started before `on_dag_run_running`")
            return
        self.executor.submit(
            self.adapter.dag_started,
            dag_run=dag_run,
            msg=msg,
            nominal_start_time=dag_run.data_interval_start.isoformat(),
            nominal_end_time=dag_run.data_interval_end.isoformat(),
        )

    @hookimpl
    def on_dag_run_success(self, dag_run: DagRun, msg: str):
        if not self.executor:
            self.log.error("Executor have not started before `on_dag_run_success`")
            return
        self.executor.submit(self.adapter.dag_success, dag_run=dag_run, msg=msg)

    @hookimpl
    def on_dag_run_failed(self, dag_run: DagRun, msg: str):
        if not self.executor:
            self.log.error("Executor have not started before `on_dag_run_failed`")
            return
        self.executor.submit(self.adapter.dag_failed, dag_run=dag_run, msg=msg)
