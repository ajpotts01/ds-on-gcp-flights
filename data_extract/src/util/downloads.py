import logging
import os
import requests
import ssl


# TODO: Port this to support requests, not urllib
def get_ssl_context() -> ssl.SSLContext:
    context: ssl.SSLContext = ssl.create_default_context()
    context.set_ciphers("HIGH:!DH:!aNULL")
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE

    return context


def download_file(year: str, month: str, target_dir: str) -> str:
    base_url: str = "https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present"
    source_filename: str = f"{year}_{month}.zip"

    target_url: str = f"{base_url}_{source_filename}"
    month_padded: str = month.rjust(2, "0")
    target_filename: str = f"{target_dir}/{year}_{month_padded}.zip"

    env: str = os.getenv("ENV", "DEV")
    verify_ssl: bool = (
        True  # Used to be env == PROD, but turns out that doesn't work on GCR either.
    )

    if not os.path.exists(target_dir):
        os.mkdir(target_dir)

    with open(file=target_filename, mode="wb") as target_file:
        logging.info(f"Requesting from: {target_url}")
        # Note: even a 20mb file takes a while to download from this site
        response: requests.Response = requests.get(url=target_url, verify=verify_ssl)
        logging.info("Writing file")
        target_file.write(response.content)
        logging.info("Done")

    return target_filename
