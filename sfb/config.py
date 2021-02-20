import os
import yaml

from google.cloud.bigquery import ScalarQueryParameter

class Config():
    def __init__(self, config_file_path: str=None):
        self.config = None
        if os.path.isfile(config_file_path):
            with open(config_file_path) as f:
                self.config = yaml.load(f, Loader=yaml.SafeLoader)
    
    def set_config(self):
        pass

class BigQueryConfig(Config):
    def __init__(self, config_file_path: str=''):
        super().__init__(config_file_path=config_file_path)

        self.location: str = None
        self.frequency: str = None
        self.query_parameters: list = []

    def __get_query_parameters(self, config: dict) -> list:
        query_parameters: list = []
        param_list: list = config.get('Parameters')

        if param_list:
            for d in param_list:
                p = ScalarQueryParameter(d['name'], d['type'], d['value'])
                query_parameters.append(p)

        return query_parameters

    def set_config(self, filepath):
        config_globals: dict = None
        config_query_file: dict = None
        key_location: str = 'Location'
        key_frequency: str = 'Frequency'

        if self.config:
            config_globals = self.config.get('Globals')
            config_query_file = self.config.get('QueryFiles').get(filepath.split('/')[-1])

        if config_globals:
            self.location = config_globals.get(key_location)
            self.frequency = config_globals.get(key_frequency)

        if config_query_file:
            if key_location in config_query_file:
                self.location = config_query_file.get(key_location)
            if key_frequency in config_query_file:
                self.frequency = config_query_file.get(key_frequency)

            self.query_parameters = self.__get_query_parameters(config_query_file)
