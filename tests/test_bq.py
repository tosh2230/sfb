import pytest

@pytest.mark.usefixtures("check_success")
class TestBQEstimator:
    def test_check_succees_type(self, check_success):
        assert type(check_success) is dict

    def test_check_succees_bytes(self, check_success):
        assert check_success['total_bytes_processed'] == 65935918

    def test_check_succees_doller(self, check_success):
        assert check_success['doller'] == 0.00029984184038767125
