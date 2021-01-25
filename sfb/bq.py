import sys

from requests.exceptions import ReadTimeout
import google.api_core.exceptions as exceptions
from google.api_core.retry import if_exception_type, Retry
from google.cloud import bigquery

TB = 1099511627776      # 1 TB
PRICING_ON_DEMAND = 5   # $5.00 per TB

class Estimator():
    def __init__(self, timeout: float=None, logger=None, conf=None) -> None:
        self.__logger = logger
        self.__client = bigquery.Client()

        __predicate = if_exception_type(
            exceptions.InternalServerError,
            exceptions.TooManyRequests,
            exceptions.ServiceUnavailable,
        )
        self.__retry = Retry(predicate=__predicate)
        self.__timeout = timeout
        self.__conf = conf

    def __get_query_conf(self, param_list: list, target: str):
        for file in param_list:
            if file.get(target) is not None:
                return file.get(target)
        return None

    def __get_query_parameters(self, query_conf):
        query_parameters = []
        for d in query_conf['Parameters']:
            param = bigquery.ScalarQueryParameter(d['name'], d['type'], d['value'])
            query_parameters.append(param)
        return query_parameters

    def check(self, filepath: str) -> dict:
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                query = f.read()

            query_parameters = []
            location = None

            if self.__conf is not None:
                param_list = self.__conf['QueryFiles']
                target = filepath.split('/')[-1]
                query_conf = self.__get_query_conf(param_list=param_list, target=target)
                if query_conf is not None:
                    query_parameters = self.__get_query_parameters(query_conf)
                    location = query_conf.get('location')

            job_config = bigquery.QueryJobConfig(
                dry_run=True,
                use_legacy_sql=False,
                use_query_cache=False,
                query_parameters=query_parameters,
            )

            query_job = self.__client.query(
                query,
                job_config=job_config,
                location=location,
                retry=self.__retry,
                timeout=self.__timeout,
            )

            dollar = query_job.total_bytes_processed / TB * PRICING_ON_DEMAND

            return {
                "sql_file": filepath,
                "query_parameter": str(query_parameters),
                "total_bytes_processed": query_job.total_bytes_processed,
                "cost($)": round(dollar, 6),
            }

        except (exceptions.BadRequest, exceptions.NotFound) as e:
            if self.__logger:
                self.__logger.exception(f'sql_file: {filepath}')
                self.__logger.exception(e, exc_info=False)
            return {
                "sql_file": filepath,
                "errors": e.errors,
            }

        except ReadTimeout as e:
            if self.__logger:
                self.__logger.exception(f'sql_file: {filepath}')
                self.__logger.exception(e, exc_info=False)
            return {
                "sql_file": filepath,
                "errors": str(e),
            }

        except KeyError as e:
            if self.__logger:
                self.__logger.exception(f'sql_file: {filepath}')
                self.__logger.exception(e, exc_info=False)
            return {
                "sql_file": filepath,
                "errors": f"Config KeyError: {e}",
            }

        except Exception as e:
            if self.__logger:
                self.__logger.exception(f'sql_file: {filepath}')
                self.__logger.exception(e, exc_info=False)
            raise e
