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

import logging
from typing import TYPE_CHECKING

import pluggy

from airflow.sdk.api.datamodels._generated import IntermediateTIState, TerminalTIState
from airflow.sdk.execution_time.comms import ActivityResult, ActivityStatus

if TYPE_CHECKING:
    from pluggy._hooks import _HookRelay

    from airflow.sdk.execution_time.task_runner import RuntimeListenerActivity

log = logging.getLogger(__name__)


_listener_manager: TIRuntimeListenerManager | None = None


def _before_hookcall(hook_name, hook_impls, kwargs):
    log.debug("Calling %r with %r", hook_name, kwargs)
    log.debug("Hook impls: %s", hook_impls)


def _after_hookcall(outcome, hook_name, hook_impls, kwargs):
    log.debug("Result from %r: %s", hook_name, outcome.get_result())


class TIRuntimeListenerManager:
    """Manage listener registration and provides hook property for calling them."""

    def __init__(self):
        from airflow.listeners.spec import lifecycle, taskinstance

        self.pm = pluggy.PluginManager("airflow")
        self.pm.add_hookcall_monitoring(_before_hookcall, _after_hookcall)
        self.pm.add_hookspecs(lifecycle)
        self.pm.add_hookspecs(taskinstance)

    @property
    def has_listeners(self) -> bool:
        return bool(self.pm.get_plugins())

    @property
    def hook(self) -> _HookRelay:
        """Return hook, on which plugin methods specified in spec can be called."""
        return self.pm.hook

    def add_listener(self, listener):
        if self.pm.is_registered(listener):
            return
        self.pm.register(listener)

    def clear(self):
        """Remove registered plugins."""
        for plugin in self.pm.get_plugins():
            self.pm.unregister(plugin)


def get_listener_manager() -> TIRuntimeListenerManager:
    """Get singleton listener manager."""
    global _listener_manager
    if not _listener_manager:
        _listener_manager = TIRuntimeListenerManager()

        # TODO: get the import path for listeners directly? No need to run whole plugin machinery
        from airflow.plugins_manager import integrate_listener_plugins

        integrate_listener_plugins(_listener_manager)
    return _listener_manager


def get_listener_hook(state: TerminalTIState | IntermediateTIState) -> callable:
    if state is IntermediateTIState.QUEUED:
        hook = get_listener_manager().hook.on_task_instance_running
    elif state is TerminalTIState.SUCCESS:
        hook = get_listener_manager().hook.on_task_instance_success
    elif state is TerminalTIState.FAILED:
        hook = get_listener_manager().hook.on_task_instance_failed
    elif state is TerminalTIState.SKIPPED:
        hook = get_listener_manager().hook.on_task_instance_skipped
    elif state is IntermediateTIState.UP_FOR_RETRY:
        hook = get_listener_manager().hook.on_task_instance_retry
    else:
        ...
    return hook


def execute_listeners(
    state: TerminalTIState | IntermediateTIState, activity: RuntimeListenerActivity, log: logging.Logger
) -> ActivityResult:
    # TODO: Running is only _outside_ TaskSDK
    hook = get_listener_hook(state)
    try:
        hook(previous_state=state, task_instance=activity.runtime_ti)
        return ActivityResult(id="", status=ActivityStatus.SUCCESS)
    except Exception:
        log.exception("Error in listener")
        return ActivityResult(id="", status=ActivityStatus.FAILED)
