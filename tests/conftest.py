import pytest
import os

from sfb.estimator import BigQueryEstimator
from sfb.config import BigQueryConfig

current_dir = os.path.dirname(os.path.abspath(__file__))

sql_file_success = f'{current_dir}/sql/test_success.sql'
sql_file_failure_nf = f'{current_dir}/sql/test_failure_notfound.sql'
sql_file_failure_br_01 = f'{current_dir}/sql/test_failure_badrequest_01.sql'
sql_file_failure_br_02 = f'{current_dir}/sql/test_failure_badrequest_02.sql'

@pytest.fixture(scope="session")
def estimator():
    config = BigQueryConfig()
    estimator = BigQueryEstimator(config=config)
    return estimator

@pytest.fixture(scope="function")
def check_file_success(estimator):
    return estimator.check_file(sql_file_success)

@pytest.fixture(scope="function")
def check_file_failure_notfound(estimator):
    return estimator.check_file(sql_file_failure_nf)

@pytest.fixture(scope="function")
def check_file_failure_badrequest_01(estimator):
    return estimator.check_file(sql_file_failure_br_01)

@pytest.fixture(scope="function")
def check_file_failure_badrequest_02(estimator):
    return estimator.check_file(sql_file_failure_br_02)
