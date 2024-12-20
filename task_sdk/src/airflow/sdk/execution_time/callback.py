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

import contextlib
import logging
from typing import TYPE_CHECKING, Any

from airflow.sdk.api.datamodels._generated import IntermediateTIState, TerminalTIState
from airflow.sdk.execution_time.comms import ActivityResult, ActivityStatus

if TYPE_CHECKING:
    from airflow.sdk.execution_time.task_runner import RuntimeTaskCallbackActivity


def execute_task_callback(
    state: TerminalTIState | IntermediateTIState, activity: RuntimeTaskCallbackActivity, log: logging.Logger
) -> ActivityResult:
    if state is TerminalTIState.SUCCESS:
        callback = activity.runtime_ti.task.on_success_callback
    elif state is TerminalTIState.FAILED:
        callback = activity.runtime_ti.task.on_failure_callback
    elif state is TerminalTIState.SKIPPED:
        callback = activity.runtime_ti.task.on_skipped_callback
    elif state is IntermediateTIState.UP_FOR_RETRY:
        callback = activity.runtime_ti.task.on_retry_callback
    else:
        raise ValueError(f"Unknown task callback type: {activity.task_callback_type}")

    def get_callback_representation(callback) -> Any:
        with contextlib.suppress(AttributeError):
            return callback.__name__
        with contextlib.suppress(AttributeError):
            return callback.__class__.__name__
        return callback

    callback_repr = get_callback_representation(callback)
    log.info("Executing callback %s", callback_repr)
    try:
        callback(activity.runtime_ti.get_template_context())
        return ActivityResult(
            id="",  # TODO: provide ID
            status=ActivityStatus.SUCCESS,
        )
    except Exception:
        log.exception("Error in callback %s", callback_repr)
        return ActivityResult(id="", status=ActivityStatus.FAILED)
