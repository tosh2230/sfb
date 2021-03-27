"""
Configuration
"""
import os
import yaml

from google.cloud.bigquery import ScalarQueryParameter


class Config():
    def __init__(self, config_file_path: str = None):
        self.config = None
        if config_file_path and os.path.isfile(config_file_path):
            with open(config_file_path) as file:
                self.config = yaml.load(file, Loader=yaml.SafeLoader)

    def set_config(self, filepath):
        pass


class BigQueryConfig(Config):
    def __init__(self, config_file_path: str = ''):
        super().__init__(config_file_path=config_file_path)

        self.location = None
        self.frequency = None
        self.query_parameters: list = []

    @staticmethod
    def __get_query_params(config: dict) -> list:
        query_parameters = []
        param_list = config.get('Parameters')

        if param_list:
            for dic in param_list:
                param = ScalarQueryParameter(
                    dic['name'], dic['type'], dic['value']
                )
                query_parameters.append(param)

        return query_parameters

    def set_config(self, filepath):
        conf_globals: dict = None
        conf_query_file: dict = None
        key_location: str = 'Location'
        key_frequency: str = 'Frequency'

        if self.config:
            conf_globals = self.config.get('Globals')
            conf_query_file = self.config.get('QueryFiles') \
                .get(filepath.split('/')[-1])

        if conf_globals:
            self.location = conf_globals.get(key_location)
            self.frequency = conf_globals.get(key_frequency)

        if conf_query_file:
            if key_location in conf_query_file:
                self.location = conf_query_file.get(key_location)
            if key_frequency in conf_query_file:
                self.frequency = conf_query_file.get(key_frequency)

            self.query_parameters = self.__get_query_params(conf_query_file)
