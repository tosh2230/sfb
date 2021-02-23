import os
import pytest
from requests.exceptions import ReadTimeout
from google.api_core.exceptions import BadRequest, NotFound

class TestBigQueryVanillaSuccees:
    def test_check_file_type(self, plain_vanilla_success):
        assert type(plain_vanilla_success) is dict

    def test_check_file_path(self, plain_vanilla_success):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        assert plain_vanilla_success['Result']['SQL File'] == f'{current_dir}/sql/test_success.sql'

    def test_check_file_bytes(self, plain_vanilla_success):
        assert plain_vanilla_success['Result']['Total Bytes Processed'][-3:] == 'MiB'

    def test_check_file_cost_per_run(self, plain_vanilla_success):
        assert plain_vanilla_success['Result']['Estimated Cost($)']['per Run'] > 0

class TestBigQueryVanillaFailure:
    def test_check_file_notfound_type(self, plain_vanilla_failure_notfound):
        assert 'Not found' in plain_vanilla_failure_notfound['Result']['Errors'][0]['message']

    def test_check_file_badrequest_01(self, plain_vanilla_failure_badrequest_01):
        assert 'Unrecognized name' in plain_vanilla_failure_badrequest_01['Result']['Errors'][0]['message']

    def test_check_file_badrequest_02(self, plain_vanilla_failure_badrequest_02):
        assert 'Syntax error' in plain_vanilla_failure_badrequest_02['Result']['Errors'][0]['message']

class TestBigQueryConfiguredSuccees:
    def test_check_file_type(self, configured_success):
        assert type(configured_success) is dict

    def test_check_file_bytes(self, configured_success):
        assert configured_success['Result']['Total Bytes Processed'][-3:] == 'GiB'

    def test_check_file_cost_per_run(self, configured_success):
        assert configured_success['Result']['Estimated Cost($)']['per Run'] > 0

    def test_check_file_cost_per_month(self, configured_success):
        assert configured_success['Result']['Estimated Cost($)']['per Month'] > 0

    def test_check_file_frequency(self, configured_success):
        assert configured_success['Result']['Frequency'] == 'Weekly'

class TestBigQueryConfiguredFailure:
    def test_check_file_notfound_type(self, configured_failure_notfound):
        assert 'Not found' in configured_failure_notfound['Result']['Errors'][0]['message']

    def test_check_file_badrequest_01(self, configured_failure_badrequest_01):
        assert 'Unrecognized name' in configured_failure_badrequest_01['Result']['Errors'][0]['message']

    def test_check_file_badrequest_02(self, configured_failure_badrequest_02):
        assert 'Syntax error' in configured_failure_badrequest_02['Result']['Errors'][0]['message']

class TestBigQueryVanillaQuerySuccees:
    def test_check_file_type(self, plain_vanilla_query_success):
        assert type(plain_vanilla_query_success) is dict

    def test_check_file_bytes(self, plain_vanilla_query_success):
        assert plain_vanilla_query_success['Result']['Total Bytes Processed'][-3:] == 'MiB'

    def test_check_file_cost_per_run(self, plain_vanilla_query_success):
        assert plain_vanilla_query_success['Result']['Estimated Cost($)']['per Run'] > 0

class TestBigQueryVanillaQueryFailure:
    def test_check_file_notfound_type(self, plain_vanilla_query_failure_notfound):
        assert 'Not found' in plain_vanilla_query_failure_notfound['Result']['Errors'][0]['message']

    def test_check_file_badrequest_01(self, plain_vanilla_query_failure_badrequest_01):
        assert 'Unrecognized name' in plain_vanilla_query_failure_badrequest_01['Result']['Errors'][0]['message']

    def test_check_file_badrequest_02(self, plain_vanilla_query_failure_badrequest_02):
        assert 'Syntax error' in plain_vanilla_query_failure_badrequest_02['Result']['Errors'][0]['message']
