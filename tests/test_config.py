from google.cloud.bigquery import ScalarQueryParameter


class TestVanillaConfig:
    def test_init(self, plain_vanilla_config):
        assert plain_vanilla_config.config is None


class TestBigQueryConfig:
    def test_type(self, bigquery_config):
        assert type(bigquery_config.config) is dict

    def test_set_config_location(self, bigquery_config_set):
        assert bigquery_config_set.location == 'US'

    def test_set_config_frequency(self, bigquery_config_set):
        assert bigquery_config_set.frequency == 'Weekly'

    def test_set_config_query_params_type(self, bigquery_config_set):
        assert type(bigquery_config_set.query_parameters[0]) \
            is ScalarQueryParameter

    def test_set_config_query_params_name(self, bigquery_config_query_params):
        assert bigquery_config_query_params[0]['name'] == 'ds_start_date'

    def test_set_config_query_params_parameterType(
        self, bigquery_config_query_params
    ):
        assert bigquery_config_query_params[0]['parameterType']['type'] \
             == 'DATE'

    def test_set_config_query_params_parameterValue(
        self, bigquery_config_query_params
    ):
        assert bigquery_config_query_params[0]['parameterValue']['value'] \
             == '2020-01-01'
