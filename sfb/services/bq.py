from logging import Logger

from requests.exceptions import ReadTimeout
from google.api_core.exceptions import BadRequest, NotFound
from google.cloud.bigquery import ScalarQueryParameter, QueryJobConfig, QueryJob

from .estimator import Estimator

BYTE_UNIT_LIST = ['iB', 'KiB', 'MiB', 'GiB', 'TiB']
UNIT = 2 ** 10
PRICING_UNIT = 2 ** (10 * BYTE_UNIT_LIST.index('TiB'))  # 1 TiB
PRICING_ON_DEMAND = 5                                   # $5.00 per TiB
FREQUENCY_DICT = {
    "Hourly": 24 * 30,
    "Daily": 30,
    "Weekly": 4,
    "Monthly": 1,
}

class BigQueryEstimator(Estimator):

    def __init__(self, config_query_files: dict=None, project: str=None, logger: Logger=None, verbose: bool=False) -> None:
        super().__init__(config_query_files=config_query_files, project=project, logger=logger, verbose=verbose)

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
            # timeout=self._timeout,
        )

        return query_job
 
    def __estimate_cost(self, total_bytes_processed: float) -> float:
        return round((total_bytes_processed / PRICING_UNIT * PRICING_ON_DEMAND), 6)

    def __get_readable_query(self) -> str:
        return self.__query.replace('    ', ' ').replace('\n', ' ').expandtabs(tabsize=4)

    def __get_readable_size(self, bytes: float, exp: int=0) -> str:
        if bytes < UNIT:
            rounded_bytes: float = round(bytes, 1)
            return f'{rounded_bytes} {BYTE_UNIT_LIST[exp]}'

        next_bytes: float = bytes / UNIT
        return self.__get_readable_size(bytes=next_bytes, exp=exp+1)

    def __log_exception(self, filepath: str, e: Exception) -> None:
        self._logger.exception(f'sql_file: {filepath}')
        self._logger.exception(e)

    def check_file(self, filepath: str) -> dict:
        try:
            if self._config_query_files:
                file_name: str = filepath.split('/')[-1]
                config_query_file: dict = self._config_query_files.get(file_name)

                if config_query_file:
                    self.__query_parameters = self.__get_query_parameters(config_query_file)
                    self.__location = config_query_file.get('Location')
                    self._frequency: str = config_query_file.get('Frequency')

            with open(filepath, 'r', encoding='utf-8') as f:
                self.__query = f.read()

            query_job: QueryJob = self.__execute()

            processed_size: float = self.__get_readable_size(bytes=query_job.total_bytes_processed)
            estimated: float = self.__estimate_cost(query_job.total_bytes_processed)

            response: dict = {
                "Status": "Succeeded",
                "Result": {
                    "SQL File": filepath,
                    "Total Bytes Processed": processed_size,
                    "Estimated Cost($)": {
                        "per Run": estimated,
                    }
                }
            }

            if self._frequency:
                coefficient: int = FREQUENCY_DICT[self._frequency]
                cost_per_month: float = round(estimated * coefficient, 6)
                response['Result']['Frequency'] = self._frequency
                response['Result']['Estimated Cost($)']['per Month'] = cost_per_month

            if self._verbose:
                map_repr: list = [x.to_api_repr() for x in self.__query_parameters]
                response['Result']['Query'] = self.__get_readable_query()
                response['Result']['Query Parameters'] = list(map_repr)

            return response

        except (BadRequest, NotFound) as e:
            if self._logger:
                self.__log_exception(filepath=filepath, e=e)
            return {
                "Status": "Failed",
                "Result": {
                    "SQL File": filepath,
                    "Errors": e.errors,
                }
            }

        except (ReadTimeout, KeyError) as e:
            if self._logger:
                self.__log_exception(filepath=filepath, e=e)
            return {
                "Status": "Failed",
                "Result": {
                    "SQL File": filepath,
                    f"{e.__class__.__name__}": f"{str(e)}",
                }
            }

        except Exception as e:
            if self._logger:
                self.__log_exception(filepath=filepath, e=e)
            raise e

    def check_query(self, query: str) -> dict:
        try:
            self.__query = query
            query_job: QueryJob = self.__execute()

            processed_size: float = self.__get_readable_size(bytes=query_job.total_bytes_processed)
            estimated: float = self.__estimate_cost(query_job.total_bytes_processed)
            response: dict = {
                "Status": "Succeeded",
                "Result": {
                    "Total Bytes Processed": processed_size,
                    "Estimated Cost($)": {
                        "per Run": estimated,
                    }
                }
            }

            if self._verbose:
                response['Result']['Query'] = self.__get_readable_query()

            return response

        except (BadRequest, NotFound) as e:
            if self._logger:
                self._logger.error(e)
            return {
                "Status": "Failed",
                "Result": {
                    "Errors": e.errors,
                }
            }

        except (ReadTimeout, KeyError) as e:
            if self._logger:
                self._logger.error(e)
            return {
                "Status": "Failed",
                "Result": {
                    "Errors": str(e),
                }
            }

        except Exception as e:
            if self._logger:
                self._logger.exception(e, exc_info=True)
            raise e
