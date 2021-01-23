import pytest
from requests.exceptions import ReadTimeout
from google.api_core.exceptions import BadRequest, NotFound

class TestBigQueryEstimatorSuccees:
    def test_check_type(self, check_success):
        assert type(check_success) is dict

    def test_check_bytes(self, check_success):
        assert check_success['total_bytes_processed'] == 65935918

    def test_check_doller(self, check_success):
        assert check_success['doller'] == 0.00029984184038767125

class TestBigQueryEstimatorFailure:
    def test_check_notfound_type(self, check_failure_notfound):
        assert type(check_failure_notfound) is NotFound

    def test_check_badrequest_01(self, check_failure_badrequest_01):
        assert type(check_failure_badrequest_01) is BadRequest

    def test_check_badrequest_02(self, check_failure_badrequest_02):
        assert type(check_failure_badrequest_02) is BadRequest

    def test_check_timeout(self, check_failure_timeout):
        assert type(check_failure_timeout) is ReadTimeout