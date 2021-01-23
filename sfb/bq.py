import sys
import argparse

from requests.exceptions import ReadTimeout
import google.api_core.exceptions as exceptions
from google.api_core.retry import if_exception_type, Retry
from google.cloud import bigquery

TB = 1099511627776      # 1 TB
PRICING_ON_DEMAND = 5   # $5.00 per TB

class Estimator():
    def __init__(self, timeout: float) -> None:
        self.__client = bigquery.Client()
        self.__job_config = bigquery.QueryJobConfig(
            dry_run=True,
            use_legacy_sql=False,
            use_query_cache=False,
            query_parameters=[],
        )

        __predicate = if_exception_type(
            exceptions.InternalServerError,
            exceptions.TooManyRequests,
            exceptions.ServiceUnavailable,
        )
        self.__retry = Retry(predicate=__predicate)
        self.__timeout = timeout

    def check(self, filepath: str) -> dict:
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                __query = f.read()

            __query_job = self.__client.query(
                __query,
                job_config=self.__job_config,
                retry=self.__retry,
                timeout=self.__timeout,
            )

            __dollar = __query_job.total_bytes_processed / TB * PRICING_ON_DEMAND

            return {
                "total_bytes_processed": __query_job.total_bytes_processed,
                "doller": __dollar,
            }

        except (exceptions.BadRequest, exceptions.NotFound, ReadTimeout) as e:
            print(e)
            return e
        except Exception as e:
            raise e

if __name__ == "__main__":
    __parser = argparse.ArgumentParser()
    __parser.add_argument(
        "sql_file_path", 
        help="sql file path",
        type=str,
    )
    __parser.add_argument(
        "-t", "--timeout", 
        help="request timeout seconds",
        type=float,
    )
    __args = __parser.parse_args()

    print(f"target_file: {__args.sql_file_path}")
    __estimator = Estimator(timeout=__args.timeout)
    __response = __estimator.check(__args.sql_file_path)
    print(__response)