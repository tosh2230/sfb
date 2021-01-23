import pytest
import os

from sfb.bq import Estimator

current_dir = os.path.dirname(os.path.abspath(__file__))

sql_file_success = f'{current_dir}/sql/test_success.sql'
sql_file_failure_nf = f'{current_dir}/sql/test_failure_notfound.sql'
sql_file_failure_br_01 = f'{current_dir}/sql/test_failure_badrequest_01.sql'
sql_file_failure_br_02 = f'{current_dir}/sql/test_failure_badrequest_02.sql'

@pytest.fixture(scope="session")
def estimator():
    estimator = Estimator(timeout=None)
    return estimator

@pytest.fixture(scope="session")
def check_success(estimator):
    return estimator.check(sql_file_success)

@pytest.fixture(scope="session")
def check_failure_notfound(estimator):
    return estimator.check(sql_file_failure_nf)

@pytest.fixture(scope="session")
def check_failure_badrequest_01(estimator):
    return estimator.check(sql_file_failure_br_01)

@pytest.fixture(scope="session")
def check_failure_badrequest_02(estimator):
    return estimator.check(sql_file_failure_br_02)

@pytest.fixture(scope="session")
def check_failure_timeout():
    estimator_timeout = Estimator(timeout=0.1)
    return estimator_timeout.check(sql_file_success)