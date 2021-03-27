import os
import sys
import argparse
from logging import Logger

from sfb.estimator import BigQueryEstimator
from sfb.config import Config, BigQueryConfig
from sfb.logger import SfbLogger

CONFIG_FILE = 'sfb.yaml'
BQ = 'BigQuery'

class EntryPoint():

    def __init__(self):
        self.__args: argparse.Namespace = self.__get_args()
        self.__config: dict = None
        self.__logger: Logger = None
        self.__stdin: tuple = None

        if self.__args.config:
            self.__config = Config(self.__args.config)

        if self.__args.debug:
            sfb_logger = SfbLogger()
            sfb_logger.set_logger()
            self.__logger = sfb_logger.logger

        if not sys.stdin.isatty():
            self.__stdin = self.__get_stdin()

    def __get_args(self) -> argparse.Namespace:
        parser = argparse.ArgumentParser()

        # choose either
        group = parser.add_mutually_exclusive_group()
        group.add_argument(
            "-f", "--file", help="sql filepath", type=str, nargs='*'
        )
        group.add_argument(
            "-q", "--query", help="query string", type=str, default=None
        )

        parser.add_argument(
            "-c", "--config",
            help="config filepath", type=str, default=f"./config/{CONFIG_FILE}"
        )
        # parser.add_argument(
        #     "-s", "--source",
        #     help="source type", type=str, choices=[BQ], default=BQ
        # )
        parser.add_argument(
            '-v', '--verbose',
            help="verbose results", action='store_true', default=False
        )
        parser.add_argument(
            '-d', '--debug',
            help="run as debug mode", action='store_true', default=False
        )
        parser.add_argument(
            "-p", "--project",
            help="GCP project", type=str, default=None
        )
        parser.add_argument(
            "-k", "--key",
            help="GCP service account key path", type=str, default=None
        )

        return parser.parse_args()
    
    def __get_stdin(self) -> tuple:
        list_stdin = []
        for line in sys.stdin:
            list_stdin.append(line.rstrip())

        return tuple(list_stdin)

    def execute(self) -> dict:
        try:
            estimator = BigQueryEstimator(
                logger=self.__logger,
                verbose=self.__args.verbose,
                config=BigQueryConfig(self.__args.config),
                project=self.__args.project,
                key=self.__args.key
            )

            files: list = []
            queries: list = []
            sql_dir = os.getcwd() + '/sql'

            if self.__stdin:
                for line in self.__stdin:
                    if line[-4:] == '.sql':
                        files.append(line)
                    elif 'SELECT' in line.upper() or 'FROM' in line.upper():
                        queries.append(line)
            elif self.__args.file:
                files = self.__args.file
            elif self.__args.query:
                queries.append(self.__args.query)
            elif os.path.isdir(sql_dir):
                files = [f'{sql_dir}/{file}' for file in os.listdir(sql_dir)]

            for file in files:
                yield estimator.check_file(file)

            for query in queries:
                yield estimator.check_query(query)

        except (FileNotFoundError, KeyError) as e:
            if self.__logger:
                self.__logger.exception(e, exc_info=False)
            raise e

        except Exception as e:
            if self.__logger:
                self.__logger.exception(e, exc_info=True)
            raise e
