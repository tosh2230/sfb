from logging import Logger

from requests.exceptions import ReadTimeout
from google.api_core.exceptions import BadRequest, NotFound
from google.cloud.bigquery import ScalarQueryParameter, QueryJobConfig, QueryJob

from .estimator import Estimator

UNIT_SIZE = 1099511627776       # 1 TB
PRICING_ON_DEMAND = 5           # $5.00 per TB
FREQUENCY_DICT = {
    "Hourly": 24 * 30,
    "Daily": 30,
    "Weekly": 4,
    "Monthly": 1,
}

class BigQueryEstimator(Estimator):

    def __init__(self, config_query_files: dict=None, logger: Logger=None, timeout: float=None, verbose: bool=False) -> None:
        super().__init__(config_query_files=config_query_files, timeout=timeout, logger=logger, verbose=verbose)

        self.__query_parameters: list = []
        self.__location: str = None
        self.__query: str = None

    def __get_query_parameters(self, config: dict) -> list:
        query_parameters: list = []
        for d in config['Parameters']:
            p = ScalarQueryParameter(d['name'], d['type'], d['value'])
            query_parameters.append(p)

        return query_parameters

    def __execute(self) -> QueryJob:
        job_config = QueryJobConfig(
            dry_run=True,
            use_legacy_sql=False,
            use_query_cache=False,
            query_parameters=self.__query_parameters,
        )

        query_job = self._client.query(
            self.__query,
            job_config=job_config,
            location=self.__location,
            retry=self._retry,
            timeout=self._timeout,
        )

        return query_job
 
    def __estimate_cost(self, total_bytes_processed: float) -> float:
        return round((total_bytes_processed / UNIT_SIZE * PRICING_ON_DEMAND), 6)

    def __get_readable_query(self) -> str:
        return self.__query.replace('    ', ' ').replace('\n', ' ').expandtabs(tabsize=4)

    def __log_error(self, filepath: str, e: Exception) -> None:
        self._logger.error(f'sql_file: {filepath}')
        self._logger.error(e, exc_info=False)

    def check_file(self, filepath: str) -> dict:
        try:
            if self._config_query_files:
                file_name: str = filepath.split('/')[-1]
                config_query_file: dict = self._config_query_files[file_name]
                self.__query_parameters = self.__get_query_parameters(config_query_file)
                self.__location = config_query_file.get('Location')
                frequency: str = config_query_file.get('Frequency')

            with open(filepath, 'r', encoding='utf-8') as f:
                self.__query = f.read()

            query_job: QueryJob = self.__execute()

            estimated: float = self.__estimate_cost(query_job.total_bytes_processed)
            map_repr: list = [x.to_api_repr() for x in self.__query_parameters]

            result: dict = {
                "SQL File": filepath,
                "Status": "Succeeded",
                "Total Bytes Processed": query_job.total_bytes_processed,
                "Estimated Cost($)": {
                    "per Run": estimated,
                }
            }

            if frequency:
                coefficient: int = FREQUENCY_DICT[frequency]
                cost_per_month: float = round(estimated * coefficient, 6)
                result['Frequency'] = frequency
                result['Estimated Cost($)']['per Month'] = cost_per_month

            if self._verbose:
                result['Query'] = self.__get_readable_query()
                result['Query Parameters'] = list(map_repr)

            return result

        except (BadRequest, NotFound) as e:
            if self._logger:
                self.__log_error(filepath=filepath, e=e)
            return {
                "SQL File": filepath,
                "Status": "Failed",
                "Errors": e.errors,
            }

        except (ReadTimeout, KeyError) as e:
            if self._logger:
                self.__log_error(filepath=filepath, e=e)
            return {
                "SQL File": filepath,
                "Status": "Failed",
                "Errors": str(e),
            }

        except Exception as e:
            if self._logger:
                self._logger.exception(f'sql_file: {filepath}')
                self._logger.exception(e, exc_info=True)
            raise e

    def check_query(self, query: str) -> dict:
        try:
            self.__query = query
            query_job: QueryJob = self.__execute()

            estimated: float = self.__estimate_cost(query_job.total_bytes_processed)
            result: dict = {
                "Status": "Succeeded",
                "Total Bytes Processed": query_job.total_bytes_processed,
                "Estimated Cost($)": {
                    "per Run": estimated,
                }
            }

            if self._verbose:
                result['Query'] = self.__get_readable_query()

            return result

        except (BadRequest, NotFound) as e:
            if self._logger:
                self._logger.error(e)
            return {
                "Status": "Failed",
                "Errors": e.errors,
            }

        except (ReadTimeout, KeyError) as e:
            if self._logger:
                self._logger.error(e)
            return {
                "Status": "Failed",
                "Errors": str(e),
            }

        except Exception as e:
            if self._logger:
                self._logger.exception(e, exc_info=True)
            raise e