import google.api_core.exceptions as exceptions
from google.api_core.retry import if_exception_type, Retry
from google.cloud import bigquery

class Estimator():
    def __init__(self, timeout: float=None, logger=None, config=None):
        self._logger = logger
        self._client = bigquery.Client()

        predicate = if_exception_type(
            exceptions.InternalServerError,
            exceptions.TooManyRequests,
            exceptions.ServiceUnavailable,
        )
        self._retry = Retry(predicate=predicate)
        self._timeout = timeout
        self._config = config

def check(self, filepath: str) -> dict:
    pass