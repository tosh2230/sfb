from logging import Logger

from google.api_core.exceptions import InternalServerError, TooManyRequests, ServiceUnavailable
from google.api_core.retry import if_exception_type, Retry
from google.cloud import bigquery

class Estimator():

    def __init__(self, timeout: float=None, logger: Logger=None, config_query_files: dict=None):
        self._client = bigquery.Client()
        self._timeout = timeout
        self._logger = logger
        self._config_query_files = config_query_files

        predicate = if_exception_type(
            InternalServerError,
            TooManyRequests,
            ServiceUnavailable,
        )
        self._retry = Retry(predicate=predicate)

    def check(self, filepath: str) -> dict:
        pass