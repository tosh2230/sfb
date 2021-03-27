import os


class TestBigQueryVanillaSuccees:
    def test_check_file_type(self, plain_vanilla_success):
        assert type(plain_vanilla_success) is dict

    def test_check_file_path(self, plain_vanilla_success):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        result = plain_vanilla_success['Result']
        assert result['SQL File'] == f'{current_dir}/sql/test_success.sql'

    def test_check_file_bytes(self, plain_vanilla_success):
        result = plain_vanilla_success['Result']
        assert result['Total Bytes Processed'][-3:] == 'MiB'

    def test_check_file_cost_per_run(self, plain_vanilla_success):
        result = plain_vanilla_success['Result']
        assert result['Estimated Cost($)']['per Run'] > 0


class TestBigQueryVanillaFailure:
    def test_check_file_notfound_type(self, plain_vanilla_failure_notfound):
        result = plain_vanilla_failure_notfound['Result']
        message = result['Errors'][0]['message']
        assert 'Not found' in message

    def test_check_file_badrequest_01(
        self, plain_vanilla_failure_badrequest_01
    ):
        result = plain_vanilla_failure_badrequest_01['Result']
        message = result['Errors'][0]['message']
        assert 'Unrecognized name' in message

    def test_check_file_badrequest_02(
        self, plain_vanilla_failure_badrequest_02
    ):
        result = plain_vanilla_failure_badrequest_02['Result']
        message = result['Errors'][0]['message']
        assert 'Syntax error' in message


class TestBigQueryConfiguredSuccees:
    def test_check_file_type(self, configured_success):
        assert type(configured_success) is dict

    def test_check_file_bytes(self, configured_success):
        total_bytes = configured_success['Result']['Total Bytes Processed']
        assert total_bytes[-3:] == 'GiB'

    def test_check_file_cost_per_run(self, configured_success):
        costs = configured_success['Result']['Estimated Cost($)']
        assert costs['per Run'] > 0

    def test_check_file_cost_per_month(self, configured_success):
        costs = configured_success['Result']['Estimated Cost($)']
        assert costs['per Month'] > 0

    def test_check_file_frequency(self, configured_success):
        assert configured_success['Result']['Frequency'] == 'Weekly'


class TestBigQueryConfiguredFailure:
    def test_check_file_notfound_type(self, configured_failure_notfound):
        result = configured_failure_notfound['Result']
        assert 'Not found' in result['Errors'][0]['message']

    def test_check_file_badrequest_01(self, configured_failure_badrequest_01):
        result = configured_failure_badrequest_01['Result']
        assert 'Unrecognized name' in result['Errors'][0]['message']

    def test_check_file_badrequest_02(self, configured_failure_badrequest_02):
        result = configured_failure_badrequest_02['Result']
        assert 'Syntax error' in result['Errors'][0]['message']


class TestBigQueryVanillaQuerySuccees:
    def test_check_file_type(self, plain_vanilla_query_success):
        assert type(plain_vanilla_query_success) is dict

    def test_check_file_bytes(self, plain_vanilla_query_success):
        result = plain_vanilla_query_success['Result']
        assert result['Total Bytes Processed'][-3:] == 'MiB'

    def test_check_file_cost_per_run(self, plain_vanilla_query_success):
        result = plain_vanilla_query_success['Result']
        assert result['Estimated Cost($)']['per Run'] > 0


class TestBigQueryVanillaQueryFailure:
    def test_check_file_notfound_type(
        self, plain_vanilla_query_failure_notfound
    ):
        result = plain_vanilla_query_failure_notfound['Result']
        assert 'Not found' in result['Errors'][0]['message']

    def test_check_file_badrequest_01(
        self, plain_vanilla_query_failure_badrequest_01
    ):
        result = plain_vanilla_query_failure_badrequest_01['Result']
        assert 'Unrecognized name' in result['Errors'][0]['message']

    def test_check_file_badrequest_02(
        self, plain_vanilla_query_failure_badrequest_02
    ):
        result = plain_vanilla_query_failure_badrequest_02['Result']
        assert 'Syntax error' in result['Errors'][0]['message']
