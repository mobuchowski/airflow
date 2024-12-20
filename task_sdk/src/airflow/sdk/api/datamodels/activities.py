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

import os
from typing import Annotated, Literal, Union

from pydantic import BaseModel, Field

from airflow.sdk.api.datamodels._generated import IntermediateTIState, TaskInstance, TerminalTIState


class ExecuteTaskActivity(BaseModel):
    ti: TaskInstance
    path: os.PathLike[str]
    token: str
    """The identity token for this workload"""


class ListenerActivity(BaseModel):
    state: TerminalTIState | IntermediateTIState
    type: Literal["ListenerActivity"] = "ListenerActivity"


class TaskCallbackActivity(BaseModel):
    full_filepath: str
    ti: TaskInstance
    processor_subdir: str | None = None
    msg: str | None = None
    state: TerminalTIState | IntermediateTIState | None = None
    type: Literal["CallbackActivity"] = "CallbackActivity"


Activity = Annotated[
    Union[ListenerActivity, TaskCallbackActivity],
    Field(discriminator="type"),
]
