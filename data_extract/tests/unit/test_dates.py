import pytest

from src.util.dates import get_next_month


# Test a month with 30 days
def test_next_month_thirty_days():
    input_year: str = "2023"
    input_month: str = "4"
    want_year: str = "2023"
    want_month: str = "5"
    got_year: str
    got_month: str

    got_year, got_month = get_next_month(year=input_year, month=input_month)

    assert got_year == want_year and got_month == want_month


# Test a month with 31 days
def test_next_month_thirty_one_days():
    input_year: str = "2023"
    input_month: str = "10"
    want_year: str = "2023"
    want_month: str = "11"
    got_year: str
    got_month: str

    got_year, got_month = get_next_month(year=input_year, month=input_month)

    assert got_year == want_year and got_month == want_month


def test_next_month_roll_next_year():
    input_year: str = "2023"
    input_month: str = "12"
    want_year: str = "2024"
    want_month: str = "1"
    got_year: str
    got_month: str

    got_year, got_month = get_next_month(year=input_year, month=input_month)

    assert got_year == want_year and got_month == want_month
