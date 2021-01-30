from logging import Logger

from requests.exceptions import ReadTimeout
from google.api_core.exceptions import BadRequest, NotFound
from google.cloud.bigquery import ScalarQueryParameter, QueryJobConfig

from .estimator import Estimator

UNIT_SIZE = 1099511627776       # 1 TB
PRICING_ON_DEMAND = 5           # $5.00 per TB

class BigQueryEstimator(Estimator):

    def __init__(self, timeout: float=None, logger: Logger=None, config_query_files: dict=None) -> None:
        super().__init__(timeout, logger, config_query_files)

    def __get_query_parameters(self, config: dict) -> list:
        query_parameters = []
        for d in config['Parameters']:
            p = ScalarQueryParameter(d['name'], d['type'], d['value'])
            query_parameters.append(p)
        return query_parameters

    def __log_exception(self, filepath: str, e: Exception) -> None:
        self._logger.exception(f'sql_file: {filepath}')
        self._logger.exception(e, exc_info=False)

    def check(self, filepath: str) -> dict:
        try:
            query_parameters = []
            location = None

            with open(filepath, 'r', encoding='utf-8') as f:
                query = f.read()

            if self._config_query_files:
                file_name = filepath.split('/')[-1]
                config_query_file = self._config_query_files[file_name]
                query_parameters = self.__get_query_parameters(config_query_file)
                location = config_query_file.get('location')

            job_config = QueryJobConfig(
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
            map_repr = map(lambda x: x.to_api_repr(), query_parameters)

            return {
                "sql_file": filepath,
                "status": "succeeded",
                "total_bytes_processed": query_job.total_bytes_processed,
                "estimated_cost($)": round(estimated, 6),
                "query_parameter": list(map_repr),
            }

        except (BadRequest, NotFound) as e:
            if self._logger:
                self.__log_exception(filepath=filepath, e=e)
            return {
                "sql_file": filepath,
                "status": "failed",
                "errors": e.errors,
            }

        except (ReadTimeout, KeyError) as e:
            if self._logger:
                self.__log_exception(filepath=filepath, e=e)
            return {
                "sql_file": filepath,
                "status": "failed",
                "errors": str(e),
            }

        except Exception as e:
            if self._logger:
                self.__log_exception(filepath=filepath, e=e)
            raise e
