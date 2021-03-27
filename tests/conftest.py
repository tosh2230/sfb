import pytest
import os

from sfb.entrypoint import EntryPoint
from sfb.estimator import BigQueryEstimator
from sfb.config import BigQueryConfig
from sfb.logger import SfbLogger

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
    project = os.environ['GCP_PROJECT']
    return BigQueryEstimator(project=project, config=plain_vanilla_config)


########################################
# plain_vanilla_file
########################################
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
# configured_file
########################################
@pytest.fixture(scope="session")
def bigquery_logger():
    sfb_logger = SfbLogger()
    sfb_logger.set_logger()
    return sfb_logger.logger


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
def configured_estimator(bigquery_logger, bigquery_config):
    return BigQueryEstimator(
        config=bigquery_config,
        logger=bigquery_logger,
        verbose=True
    )


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


########################################
# query
########################################
@pytest.fixture(scope="function")
def plain_vanilla_query_success(configured_estimator):
    with open(sql_file_success, 'r', encoding='utf-8') as f:
        query = f.read()
    return configured_estimator.check_query(query)


@pytest.fixture(scope="function")
def plain_vanilla_query_failure_notfound(configured_estimator):
    with open(sql_file_failure_nf, 'r', encoding='utf-8') as f:
        query = f.read()
    return configured_estimator.check_query(query)


@pytest.fixture(scope="function")
def plain_vanilla_query_failure_badrequest_01(configured_estimator):
    with open(sql_file_failure_br_01, 'r', encoding='utf-8') as f:
        query = f.read()
    return configured_estimator.check_query(query)


@pytest.fixture(scope="function")
def plain_vanilla_query_failure_badrequest_02(configured_estimator):
    with open(sql_file_failure_br_02, 'r', encoding='utf-8') as f:
        query = f.read()
    return configured_estimator.check_query(query)


########################################
# entrypoint
########################################
@pytest.fixture(scope="session")
def bigquery_entrypoint():
    results = {"Succeeded": [], "Failed": []}
    ep = EntryPoint()
    for response in ep.execute():
        results[response['Status']].append(response['Result'])
    return results
