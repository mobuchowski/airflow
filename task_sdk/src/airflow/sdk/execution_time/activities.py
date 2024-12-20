#
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
from __future__ import annotations

from airflow.executors.workloads import ExecuteCallback, ExecuteTIListener, SubActivity
from airflow.sdk.api.datamodels.activities import Activity, ListenerActivity, TaskCallbackActivity


def from_executor(workloads: list[SubActivity]) -> list[Activity]:
    activities = []
    for workload in workloads:
        activity = None
        if isinstance(workload, ExecuteCallback):
            activity = TaskCallbackActivity.model_construct(
                full_filepath=workload.full_filepath,
                ti=workload.ti,
                processor_subdir=workload.processor_subdir,
                msg=workload.msg,
                state=workload.state,
            )
        elif isinstance(workload, ExecuteTIListener):
            activity = ListenerActivity.model_construct(
                state=workload.state,
            )
        if activity:
            activities.append(activity)
    return activities
