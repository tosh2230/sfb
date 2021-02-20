from logging import Logger

from requests.exceptions import ReadTimeout
from google.api_core.exceptions import BadRequest, NotFound, InternalServerError, TooManyRequests, ServiceUnavailable
from google.api_core.retry import if_exception_type, Retry
from google.cloud.bigquery import Client, ScalarQueryParameter, QueryJobConfig, QueryJob

from sfb.config import Config, BigQueryConfig

class Estimator():

    def __init__(self, project: str='', logger: Logger=None, verbose: bool=False) -> None:
        if project:
            self._client = Client(project)
        else:
            self._client = Client()

        self._logger = logger
        self._verbose = verbose
        self._config: Config = None

        predicate = if_exception_type(
            InternalServerError,
            TooManyRequests,
            ServiceUnavailable,
        )
        self._retry = Retry(predicate=predicate)

    def check(self, filepath: str) -> dict:
        pass


class BigQueryEstimator(Estimator):

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

    def __init__(self, project: str=None, logger: Logger=None, verbose: bool=False, config: BigQueryConfig=None) -> None:
        super().__init__(project=project, logger=logger, verbose=verbose)

        self._config: BigQueryConfig = config
        self.__query: str = None

    def __execute(self) -> QueryJob:
        job_config = QueryJobConfig(
            dry_run=True,
            use_legacy_sql=False,
            use_query_cache=False,
            query_parameters=self._config.query_parameters,
        )

        query_job = self._client.query(
            self.__query,
            job_config=job_config,
            location=self._config.location,
            retry=self._retry,
        )

        return query_job
 
    def __estimate_cost(self, total_bytes_processed: float) -> float:
        return round((total_bytes_processed / self.PRICING_UNIT * self.PRICING_ON_DEMAND), 6)

    def __get_readable_query(self) -> str:
        return self.__query.replace('    ', ' ').replace('\n', ' ').expandtabs(tabsize=4)

    def __get_readable_size(self, bytes: float, exp: int=0) -> str:
        if bytes < self.UNIT:
            rounded_bytes: float = round(bytes, 1)
            return f'{rounded_bytes} {self.BYTE_UNIT_LIST[exp]}'

        next_bytes: float = bytes / self.UNIT
        return self.__get_readable_size(bytes=next_bytes, exp=exp+1)

    def __get_cost_per_month(self, estimated: float) -> float:
        coefficient: int = self.FREQUENCY_DICT[self._config.frequency]
        return round(estimated * coefficient, 6)

    def __log_exception(self, filepath: str, e: Exception) -> None:
        self._logger.exception(f'sql_file: {filepath}')
        self._logger.exception(e)

    def check_file(self, filepath: str) -> dict:
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                self.__query = f.read()

            if self._config:
                self._config.set_config(filepath)
            query_job: QueryJob = self.__execute()

            estimated: float = self.__estimate_cost(query_job.total_bytes_processed)
            response: dict = {
                "Status": "Succeeded",
                "Result": {
                    "SQL File": filepath,
                    "Total Bytes Processed": self.__get_readable_size(bytes=query_job.total_bytes_processed),
                    "Estimated Cost($)": {
                        "per Run": estimated,
                    }
                }
            }

            if self._config.frequency:
                response['Result']['Frequency'] = self._config.frequency
                response['Result']['Estimated Cost($)']['per Month'] = self.__get_cost_per_month(estimated)

            if self._verbose:
                map_repr: list = [x.to_api_repr() for x in self._config.query_parameters]
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
