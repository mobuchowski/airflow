# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

log = logging.getLogger(__name__)

dag = DAG(dag_id='test_empty_dag',
          description='Test empty DAG',
          schedule='*/2 * * * *',
          start_date=datetime(2020, 1, 8),
          catchup=False,
          max_active_runs=1)
log.debug("dag created.")

dummy_task = EmptyOperator(
    task_id='test_empty',
    dag=dag
)
