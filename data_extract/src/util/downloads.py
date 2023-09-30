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
