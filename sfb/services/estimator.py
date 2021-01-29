import google.api_core.exceptions as exceptions
from google.api_core.retry import if_exception_type, Retry
from google.cloud import bigquery

class Estimator():
    def __init__(self, timeout: float=None, logger=None, config=None):

        self._client = bigquery.Client()
        self._timeout = timeout
        self._logger = logger
        self._config = config
        predicate = if_exception_type(
            exceptions.InternalServerError,
            exceptions.TooManyRequests,
            exceptions.ServiceUnavailable,
        )
        self._retry = Retry(predicate=predicate)


def check(self, filepath: str) -> dict:
    pass