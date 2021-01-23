import pytest
import os

from sfb.bq import Estimator

SQL_FILE_SUCCESS = '/home/admin/project/sfb/tests/sql/test_success.sql'


@pytest.fixture(scope="session")
def check_success():
    estimator = Estimator(timeout=None)
    return estimator.check(SQL_FILE_SUCCESS)
