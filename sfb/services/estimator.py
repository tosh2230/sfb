from logging import Logger

from google.api_core.exceptions import InternalServerError, TooManyRequests, ServiceUnavailable
from google.api_core.retry import if_exception_type, Retry
from google.cloud import bigquery

class Estimator():

    def __init__(self, config_query_files: dict=None, project: str=None, logger: Logger=None, verbose: bool=False) -> None:
        if project:
            self._client = bigquery.Client(project)
        else:
            self._client = bigquery.Client()

        self._config_query_files = config_query_files
        self._logger = logger
        self._verbose = verbose

        predicate = if_exception_type(
            InternalServerError,
            TooManyRequests,
            ServiceUnavailable,
        )
        self._retry = Retry(predicate=predicate)
        self._frequency: str = None

    def check(self, filepath: str) -> dict:
        pass
