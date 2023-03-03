# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
import os
import sys
from unittest.mock import patch

import pytest

from airflow.hooks import subprocess
from airflow.providers.openlineage.utils import get_location

log = logging.getLogger(__name__)


def execute_git_mock(cwd, params):
    # only mock the git revision
    log.debug("execute_git_mock()")
    if len(cwd) > 0 and params[0] == 'rev-list':
        return 'abcd1234'

    p = subprocess.Popen(['git'] + params,
                         cwd=cwd, stdout=subprocess.PIPE, stderr=None)
    p.wait(timeout=0.5)
    out, err = p.communicate()
    return out.decode('utf8').strip()


def has_git_root():
    return os.path.exists('.git') and os.path.isdir('.git')


@pytest.mark.skipif(not has_git_root(), reason="Git repo does not exists")
@patch('airflow.providers.openlineage.utils.execute_git',
       side_effect=execute_git_mock)
def test_dag_location(git_mock):
    assert 'https://github.com/apache/airflow/blob/abcd1234/' \
           'tests/providers/openlineage/test_dags/test_dummy_dag.py', \
        get_location("tests/providers/openlineage/test_dags/test_dummy_dag.py")


@pytest.mark.skipif(not has_git_root(), reason="Git repo does not exists")
@patch('airflow.providers.openlineage.utils.execute_git',
       side_effect=execute_git_mock)
def test_bad_file_path(git_mock):
    log.debug("test_bad_file_path()")
    with pytest.raises(FileNotFoundError):
        # invalid file
        get_location("does-not-exist/missing-dag.py")


if __name__ == "__main__":
    pytest.main([sys.argv[0]])
