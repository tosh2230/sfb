from logging import Logger

from requests.exceptions import ReadTimeout
from google.api_core.exceptions import BadRequest, NotFound
from google.cloud.bigquery import ScalarQueryParameter, QueryJobConfig, QueryJob

from .estimator import Estimator

BYTE_UNIT_LIST = ('iB', 'KiB', 'MiB', 'GiB', 'TiB')
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

    def __init__(self, config: dict=None, project: str=None, logger: Logger=None, verbose: bool=False) -> None:
        super().__init__(config=config, project=project, logger=logger, verbose=verbose)

        self.__query = None

    def __get_query_parameters(self, config: dict) -> list:
        query_parameters: list = []
        param_list: list = config.get('Parameters')

        if param_list:
            for d in param_list:
                p = ScalarQueryParameter(d['name'], d['type'], d['value'])
                query_parameters.append(p)

        return query_parameters

    def __execute(self, query_parameters: list, location: str) -> QueryJob:
        job_config = QueryJobConfig(
            dry_run=True,
            use_legacy_sql=False,
            use_query_cache=False,
            query_parameters=query_parameters,
        )

        query_job = self._client.query(
            self.__query,
            job_config=job_config,
            location=location,
            retry=self._retry,
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
            query_parameters: list = []
            location: str = None
            frequency: str = None
            config_query_file: dict = None
            config_globals: dict = None

            if self._config:
                file_name: str = filepath.split('/')[-1]
                config_query_file = self._config.get('QueryFiles').get(file_name)
                config_globals = self._config.get('Globals')

            if config_globals:
                location = config_globals.get('Location')
                frequency = config_globals.get('Frequency')

            if config_query_file:
                query_parameters = self.__get_query_parameters(config_query_file)
                if 'Location' in config_query_file:
                    location = config_query_file.get('Location')
                if 'Frequency' in config_query_file:
                    frequency = config_query_file.get('Frequency')

            with open(filepath, 'r', encoding='utf-8') as f:
                self.__query = f.read()

            query_job: QueryJob = self.__execute(query_parameters, location)

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

            if frequency:
                coefficient: int = FREQUENCY_DICT[frequency]
                cost_per_month: float = round(estimated * coefficient, 6)
                response['Result']['Frequency'] = frequency
                response['Result']['Estimated Cost($)']['per Month'] = cost_per_month

            if self._verbose:
                map_repr: list = [x.to_api_repr() for x in query_parameters]
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
