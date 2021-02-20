import pytest
import os

from sfb.estimator import BigQueryEstimator
from sfb.config import BigQueryConfig

current_dir = os.path.dirname(os.path.abspath(__file__))

sql_file_success = f'{current_dir}/sql/test_success.sql'
sql_file_failure_nf = f'{current_dir}/sql/test_failure_notfound.sql'
sql_file_failure_br_01 = f'{current_dir}/sql/test_failure_badrequest_01.sql'
sql_file_failure_br_02 = f'{current_dir}/sql/test_failure_badrequest_02.sql'
sql_file_bitcoin = f'{current_dir}/sql/crypto_bitcoin.transactions.sql'
config_file_path = f'{current_dir}/config/sfb.yaml'

@pytest.fixture(scope="session")
def plain_vanilla_config():
    return BigQueryConfig()

@pytest.fixture(scope="session")
def plain_vanilla_estimator(plain_vanilla_config):
    return BigQueryEstimator(config=plain_vanilla_config)

@pytest.fixture(scope="function")
def plain_vanilla_success(plain_vanilla_estimator):
    return plain_vanilla_estimator.check_file(sql_file_success)

@pytest.fixture(scope="function")
def plain_vanilla_failure_notfound(plain_vanilla_estimator):
    return plain_vanilla_estimator.check_file(sql_file_failure_nf)

@pytest.fixture(scope="function")
def plain_vanilla_failure_badrequest_01(plain_vanilla_estimator):
    return plain_vanilla_estimator.check_file(sql_file_failure_br_01)

@pytest.fixture(scope="function")
def plain_vanilla_failure_badrequest_02(plain_vanilla_estimator):
    return plain_vanilla_estimator.check_file(sql_file_failure_br_02)

########################################
@pytest.fixture(scope="session")
def bigquery_config():
    return BigQueryConfig(config_file_path=config_file_path)

@pytest.fixture(scope="session")
def bigquery_config_set(bigquery_config):
    bigquery_config.set_config(filepath=sql_file_bitcoin)
    return bigquery_config

@pytest.fixture(scope="session")
def bigquery_config_query_params(bigquery_config_set):    
    return [x.to_api_repr() for x in bigquery_config_set.query_parameters]

@pytest.fixture(scope="session")
def configured_estimator(bigquery_config):
    return BigQueryEstimator(config=bigquery_config)

@pytest.fixture(scope="function")
def configured_success(configured_estimator):
    return configured_estimator.check_file(sql_file_bitcoin)

@pytest.fixture(scope="function")
def configured_failure_notfound(configured_estimator):
    return configured_estimator.check_file(sql_file_failure_nf)

@pytest.fixture(scope="function")
def configured_failure_badrequest_01(configured_estimator):
    return configured_estimator.check_file(sql_file_failure_br_01)

@pytest.fixture(scope="function")
def configured_failure_badrequest_02(configured_estimator):
    return configured_estimator.check_file(sql_file_failure_br_02)