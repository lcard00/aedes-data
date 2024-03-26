import logging
import pandas as pd
import os
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry


def set_logging_config():
    logging.basicConfig(
        format="[%(asctime)s.%(msecs)03d] - %(levelname)-4s:  %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def http_response(url, format, max_retries=3, backoff_factor=90, log=False):
    retry_strategy = Retry(
        total=max_retries,
        status_forcelist=[429],
        allowed_methods=["GET"],
        backoff_factor=backoff_factor,
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.session()
    http.mount("https://", adapter=adapter)
    http.mount("http://", adapter=adapter)

    response = http.get(url)
    if log:
        logging.info(f"HTTP GET {url}")
        logging.info(f"HTTP Status Code: {response.status_code}")

    if format == "json":
        return pd.json_normalize(response.json())
    else:
        if log:
            logging.error(f"Invalid format {format}")


def check_dir(dir_path, log=False):
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
        if log:
            logging.info(f"Directory '{dir_path}' created...")
    else:
        if log:
            logging.info(f"Directory '{dir_path}' already exists...")
        return True


def check_file(file_path):
    exists = False
    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            lines = f.readlines()
            lines_without_space = [line.strip() for line in lines if line.strip()]
            if lines_without_space:
                exists = True

    return exists


def mask_uf(df, list_uf, log=False):
    if log:
        logging.info(f"Masking uf {list_uf}...")

    mask_ufs = df["mesorregiao_uf"].isin(list_uf)

    return mask_ufs


def mask_city(df, list_city, log=False):
    if log:
        logging.info(f"Masking city {list_city}...")

    mask_cities = df["municipio"].isin(list_city)

    return mask_cities


def get_url_resp(url, disease, geocode, format, ew_start, ew_end, year, log=False):
    params_url = (
        "&disease="
        + f"{disease}"
        + "&geocode="
        + f"{geocode}"
        + "&format="
        + f"{format}"
        + "&ew_start="
        + f"{ew_start}"
        + "&ew_end="
        + f"{ew_end}"
        + "&ey_start="
        + f"{year}"
        + "&ey_end="
        + f"{year}"
    )

    return "?".join([url, params_url])


def set_csv_path(params, log=False):
    df = params["ibge_data"]
    state = params["state"]
    
    if not state:
        return df.apply(
            lambda row: f"data/{params["country"]}/{row['mesorregiao_uf'].lower()}/{row['geocode']}",
            axis=1,
        )
    else:
        return df.apply(
        lambda row: f"data/{params["country"]}/{row['mesorregiao_uf'].lower()}",
        axis=1,
    )