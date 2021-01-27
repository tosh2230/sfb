import pytest
from requests.exceptions import ReadTimeout
from google.api_core.exceptions import BadRequest, NotFound

class TestBigQueryEstimatorSuccees:
    def test_check_type(self, check_success):
        assert type(check_success) is dict

    def test_check_bytes(self, check_success):
        assert check_success['total_bytes_processed'] == 65935918

    def test_check_doller(self, check_success):
        assert check_success['estimated_cost($)'] == 0.0003

class TestBigQueryEstimatorFailure:
    def test_check_notfound_type(self, check_failure_notfound):
        assert 'Not found' in check_failure_notfound['errors'][0]['message']

    def test_check_badrequest_01(self, check_failure_badrequest_01):
        assert 'Unrecognized name' in check_failure_badrequest_01['errors'][0]['message']

    def test_check_badrequest_02(self, check_failure_badrequest_02):
        assert 'Syntax error' in check_failure_badrequest_02['errors'][0]['message']

    def test_check_timeout(self, check_failure_timeout):
        assert 'Read timed out' in check_failure_timeout['errors']
