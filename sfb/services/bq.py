from logging import Logger

from requests.exceptions import ReadTimeout
from google.api_core.exceptions import BadRequest, NotFound
from google.cloud.bigquery import ScalarQueryParameter, QueryJobConfig

from .estimator import Estimator

UNIT_SIZE = 1099511627776       # 1 TB
PRICING_ON_DEMAND = 5           # $5.00 per TB

class BigQueryEstimator(Estimator):

    def __init__(self, config_query_files: dict=None, logger: Logger=None, timeout: float=None, verbose: bool=False) -> None:
        super().__init__(config_query_files=config_query_files, timeout=timeout, logger=logger, verbose=verbose)

    def __get_query_parameters(self, config: dict) -> list:
        query_parameters: list = []
        for d in config['Parameters']:
            p = ScalarQueryParameter(d['name'], d['type'], d['value'])
            query_parameters.append(p)
        return query_parameters

    def __log_exception(self, filepath: str, e: Exception) -> None:
        self._logger.exception(f'sql_file: {filepath}')
        self._logger.exception(e, exc_info=False)

    def check(self, filepath: str) -> dict:
        try:
            query_parameters: list = []
            location: str = None
            query: str = None

            with open(filepath, 'r', encoding='utf-8') as f:
                query = f.read()

            if self._config_query_files:
                file_name: str = filepath.split('/')[-1]
                config_query_file: dict = self._config_query_files[file_name]
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

            estimated: float = query_job.total_bytes_processed / UNIT_SIZE * PRICING_ON_DEMAND
            map_repr: list = [x.to_api_repr() for x in query_parameters]

            result: dict = {
                "sql_file": filepath,
                "status": "Succeeded",
                "total_bytes_processed": query_job.total_bytes_processed,
                "estimated_cost($)": round(estimated, 6),
            }

            if self._verbose:
                result.update(
                    {
                        "query": query.replace('    ', ' ').replace('\n', ' ').expandtabs(tabsize=4),
                        "query_parameter": list(map_repr),
                    }
                )

            return result

        except (BadRequest, NotFound) as e:
            if self._logger:
                self.__log_exception(filepath=filepath, e=e)
            return {
                "sql_file": filepath,
                "status": "Failed",
                "errors": e.errors,
            }

        except (ReadTimeout, KeyError) as e:
            if self._logger:
                self.__log_exception(filepath=filepath, e=e)
            return {
                "sql_file": filepath,
                "status": "Failed",
                "errors": str(e),
            }

        except Exception as e:
            if self._logger:
                self.__log_exception(filepath=filepath, e=e)
            raise e
