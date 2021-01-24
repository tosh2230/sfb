import sys
from datetime import datetime, timedelta, timezone

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

        self.UTC = timezone(timedelta(hours=0), 'UTC')

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

            time_elapsed = datetime.now(self.UTC) - __query_job.created

            __dollar = __query_job.total_bytes_processed / TB * PRICING_ON_DEMAND

            return {
                "sql_file": filepath,
                "total_bytes_processed": __query_job.total_bytes_processed,
                "doller": __dollar,
                "time_elapsed": str(time_elapsed),
            }

        except (exceptions.BadRequest, exceptions.NotFound) as e:
            print(e)
            return {
                "sql_file": filepath,
                "errors": e.errors,
            }
        except (ReadTimeout) as e:
            print(f'Read timeout: {filepath}')
            print(str(e))
            return {
                "sql_file": filepath,
                "errors": str(e),
            }
        except Exception as e:
            raise e
