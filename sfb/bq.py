import sys
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest, NotFound

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
            timeout=timeout,
        )

    def check(self, filepath: str) -> dict:
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                query = f.read()

            query_job = self.__client.query(
                query,
                job_config=self.__job_config,
            )

            dollar = query_job.total_bytes_processed / TB * PRICING_ON_DEMAND

            return {
                "total_bytes_processed": query_job.total_bytes_processed,
                "doller": dollar,
            }

        except (BadRequest, NotFound) as e:
            print(e)
        except Exception as e:
            raise e

if __name__ == "__main__":
    if len(sys.argv) == 2:
        __estimator = Estimator(timeout=None)
        __response = __estimator.check(sys.argv[1])
        print(__response)
    else:
        print('Please set a argument: sql_file_path')