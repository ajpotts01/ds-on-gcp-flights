import os
import pytest

from src.main import gzip_file


@pytest.fixture
def dummy_file():
    test_file_name: str = "test.csv"
    test_file_path: str = "./test_csv"

    os.mkdir(test_file_path)

    full_path: str = f"{test_file_path}/{test_file_name}"
    with open(full_path, "w") as test_file:
        test_file.write("hdr1,hdr2,hdr3")
        test_file.write("val1,val2,val3")

    yield full_path
    os.unlink(full_path)
    os.rmdir(test_file_path)


def test_gzip_file(dummy_file: str):
    expected_written_path: str = "./test_gzip/2023_2.csv.gz"

    os.mkdir("./test_gzip")

    result: str = gzip_file(
        source_path=dummy_file, target_path="./test_gzip", year=2023, month=2
    )

    assert expected_written_path == result

    os.unlink(result)
    os.rmdir("./test_gzip")
