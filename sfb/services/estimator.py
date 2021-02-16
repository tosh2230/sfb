from logging import Logger

from google.api_core.exceptions import InternalServerError, TooManyRequests, ServiceUnavailable
from google.api_core.retry import if_exception_type, Retry
from google.cloud import bigquery

class Estimator():

    def __init__(self, config: dict=None, project: str='', logger: Logger=None, verbose: bool=False) -> None:
        if project:
            self._client = bigquery.Client(project)
        else:
            self._client = bigquery.Client()

        self._config = config
        self._logger = logger
        self._verbose = verbose

        predicate = if_exception_type(
            InternalServerError,
            TooManyRequests,
            ServiceUnavailable,
        )
        self._retry = Retry(predicate=predicate)

    def check(self, filepath: str) -> dict:
        pass
