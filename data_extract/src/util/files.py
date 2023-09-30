import gzip
import os
import shutil
import zipfile


def unzip_file(source_path: str, target_path: str) -> str:
    zipped_file: zipfile.ZipFile = zipfile.ZipFile(file=source_path, mode="r")
    zipped_file.extractall(path=target_path)

    csv_path: str = f"{target_path}/{zipped_file.namelist()[0]}"
    zipped_file.close()

    return csv_path


def gzip_file(source_path: str, target_path: str, year: str, month: str) -> str:
    month_padded: str = month.rjust(2, "0")
    gzip_path: str = f"{target_path}/{year}_{month_padded}.csv.gz"

    if not os.path.exists(target_path):
        os.mkdir(target_path)

    # https://docs.python.org/3/library/gzip.html#examples-of-usage
    with open(source_path, "rb") as source_file:
        with gzip.open(gzip_path, "wb") as target_file:
            shutil.copyfileobj(source_file, target_file)

    return gzip_path
