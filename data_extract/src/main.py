import gzip
import os
import requests
import shutil
import ssl
import zipfile

from dotenv import load_dotenv

# TODO: Port this to support requests, not urllib
def get_ssl_context() -> ssl.SSLContext:
    context: ssl.SSLContext = ssl.create_default_context()
    context.set_ciphers("HIGH:!DH:!aNULL")
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE

    return context


def download_file(year: int, month: int, target_dir: str) -> str:
    base_url: str = "https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present"
    base_filename: str = f"{year}_{month}.zip"

    target_url: str = f"{base_url}_{base_filename}"
    target_filename: str = f"{target_dir}/{base_filename}"

    env: str = os.getenv("ENV", "DEV")
    verify_ssl: bool = env == "PROD"

    if not os.path.exists(target_dir):
        os.mkdir(target_dir)

    with open(file=target_filename, mode="wb") as target_file:
        print(f"Requesting from: {target_url}")
        # Note: even a 20mb file takes a while to download from this site
        response: requests.Response = requests.get(url=target_url, verify=verify_ssl)
        print("Writing file")
        target_file.write(response.content)
        print("Done")

    return target_filename


def unzip_file(source_path: str, target_path: str) -> str:
    zipped_file: zipfile.ZipFile = zipfile.ZipFile(file=source_path, mode="r")

    zipped_file.extractall(path=target_path)

    csv_path: str = f"{target_path}/{zipped_file.namelist()[0]}"
    zipped_file.close()

    return csv_path

def gzip_file(source_path: str, target_path: str) -> str:
    gzip_path: str = f"{target_path}/{source_path.replace('.csv', '.gzip')}"

    # https://docs.python.org/3/library/gzip.html#examples-of-usage
    with open(source_path, "rb") as source_file:
        with gzip.open(target_path, "wb") as target_file:
            shutil.copyfileobj(source_file, target_file)

    return gzip_path

def load_to_gcs():
    pass


def main() -> str:
    load_dotenv()
    target_dir_dl: str = "../download/"
    target_dir_csv: str = "../csv"
    target_dir_gzip: str = "../gzip"

    download_path: str = download_file(2015, 1, target_dir=target_dir_dl)
    csv_path: str = unzip_file(source_path=download_path, target_path=target_dir_csv)
    gzip_path: str = gzip_file(source_path=csv_path, target_path=target_dir_gzip)

    print(gzip_path)

if __name__ == "__main__":
    result: str = main()
