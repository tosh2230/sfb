from logging import Logger

from google.api_core.exceptions import InternalServerError, TooManyRequests, ServiceUnavailable
from google.api_core.retry import if_exception_type, Retry
from google.cloud import bigquery

class Estimator():

    def __init__(self, config_query_files: dict=None, logger: Logger=None, timeout: float=None, verbose: bool=False) -> None:
        self._client = bigquery.Client()
        self._config_query_files = config_query_files
        self._logger = logger
        self._timeout = timeout
        self._verbose = verbose

        predicate = if_exception_type(
            InternalServerError,
            TooManyRequests,
            ServiceUnavailable,
        )
        self._retry = Retry(predicate=predicate)

    def check(self, filepath: str) -> dict:
        pass
