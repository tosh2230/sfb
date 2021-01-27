import sys

from requests.exceptions import ReadTimeout
import google.api_core.exceptions as exceptions
from google.cloud import bigquery

from estimator import Estimator

UNIT_SIZE = 1099511627776       # 1 TB
PRICING_ON_DEMAND = 5           # $5.00 per TB

class BigQueryEstimator(Estimator):
    def __init__(self, timeout: float=None, logger=None, config=None) -> None:
        super().__init__(timeout, logger, config)

    def __get_query_parameters(self, config: dict) -> list:
        query_parameters = []
        for d in config['Parameters']:
            p = bigquery.ScalarQueryParameter(d['name'], d['type'], d['value'])
            query_parameters.append(p)
        return query_parameters

    def check(self, filepath: str) -> dict:
        try:
            query_parameters = []
            location = None

            with open(filepath, 'r', encoding='utf-8') as f:
                query = f.read()

            if self._config:
                config_file_list = self._config['QueryFiles']
                file_name = filepath.split('/')[-1]
                query_config = config_file_list[file_name]
                query_parameters = self.__get_query_parameters(query_config)
                location = query_config.get('location')

            job_config = bigquery.QueryJobConfig(
                dry_run=True,
                use_legacy_sql=False,
                use_query_cache=False,
                query_parameters=query_parameters,
            )

            query_job = self._client.query(
                query,
                job_config=job_config,
                location=location,
                retry=self._retry,
                timeout=self._timeout,
            )

            estimated = query_job.total_bytes_processed / UNIT_SIZE * PRICING_ON_DEMAND

            return {
                "sql_file": filepath,
                "query_parameter": str(query_parameters),
                "total_bytes_processed": query_job.total_bytes_processed,
                "estimated_cost($)": round(estimated, 6),
            }

        except (exceptions.BadRequest, exceptions.NotFound) as e:
            if self._logger:
                self._logger.exception(f'sql_file: {filepath}')
                self._logger.exception(e, exc_info=False)
            return {
                "sql_file": filepath,
                "errors": e.errors,
            }

        except ReadTimeout as e:
            if self._logger:
                self._logger.exception(f'sql_file: {filepath}')
                self._logger.exception(e, exc_info=False)
            return {
                "sql_file": filepath,
                "errors": str(e),
            }

        except KeyError as e:
            if self._logger:
                self._logger.exception(f'sql_file: {filepath}')
                self._logger.exception(e, exc_info=False)
            return {
                "sql_file": filepath,
                "errors": f"KeyError: {e}",
            }

        except Exception as e:
            if self._logger:
                self._logger.exception(f'sql_file: {filepath}')
                self._logger.exception(e, exc_info=False)
            raise e
