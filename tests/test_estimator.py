import pytest
from requests.exceptions import ReadTimeout
from google.api_core.exceptions import BadRequest, NotFound

class TestBigQueryEstimatorSuccees:
    def test_check_file_type(self, check_file_success):
        assert type(check_file_success) is dict

    def test_check_file_bytes(self, check_file_success):
        assert check_file_success['Result']['Total Bytes Processed'] == '62.9 MiB'

    def test_check_file_doller(self, check_file_success):
        assert check_file_success['Result']['Estimated Cost($)']['per Run'] == 0.0003

class TestBigQueryEstimatorFailure:
    def test_check_file_notfound_type(self, check_file_failure_notfound):
        assert 'Not found' in check_file_failure_notfound['Result']['Errors'][0]['message']

    def test_check_file_badrequest_01(self, check_file_failure_badrequest_01):
        assert 'Unrecognized name' in check_file_failure_badrequest_01['Result']['Errors'][0]['message']

    def test_check_file_badrequest_02(self, check_file_failure_badrequest_02):
        assert 'Syntax error' in check_file_failure_badrequest_02['Result']['Errors'][0]['message']
