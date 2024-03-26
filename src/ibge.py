import logging
import os
from venv import create
from src.utils import http_response, check_file, check_dir, mask_city, mask_uf
import pandas as pd


def process_ibge_data(params, log=False):
    ibge_path = params["ibge_path"]
    ibge_file_name = params["ibge_file_name"]
    list_city = params["list_city"]
    list_uf = params["list_uf"]

    check_dir(ibge_path)
    ibge_file_path = os.path.join(ibge_path, ibge_file_name)

    params["ibge_file_path"] = ibge_file_path

    if not check_file(ibge_file_path):
        create_ibge_data(params, log=log)

    if log:
        logging.info(f"Reading IBGE file at '{ibge_file_path}'...")

    df = pd.read_csv(ibge_file_path)

    if list_uf:
        mask = mask_uf(df, list_uf, log=log)
        df = df[mask]

    if list_city:
        mask = mask_city(df, list_city, log=log)
        df = df[mask]

    return df


def create_ibge_data(params, log=False):
    country = params["country"]
    url = params["ibge_api"]
    format = params["format"]
    ibge_file_path = params["ibge_file_path"]

    result = http_response(
        url=url, format=format, max_retries=5, backoff_factor=60, log=log
    )

    columns = [
        "municipio.id",
        "municipio.nome",
        "municipio.microrregiao.nome",
        "municipio.microrregiao.mesorregiao.nome",
        "municipio.microrregiao.mesorregiao.UF.sigla",
        "municipio.microrregiao.mesorregiao.UF.nome",
        "municipio.microrregiao.mesorregiao.UF.regiao.nome",
        "municipio.regiao-imediata.nome",
        "municipio.regiao-imediata.regiao-intermediaria.nome",
        "municipio.regiao-imediata.regiao-intermediaria.UF.sigla",
        "municipio.regiao-imediata.regiao-intermediaria.UF.nome",
        "municipio.regiao-imediata.regiao-intermediaria.UF.regiao.nome",
    ]
    columns_rename = {
        "municipio.id": "geocode",
        "municipio.nome": "municipio",
        "municipio.microrregiao.nome": "microrregiao",
        "municipio.microrregiao.mesorregiao.nome": "mesorregiao",
        "municipio.microrregiao.mesorregiao.UF.sigla": "mesorregiao_uf",
        "municipio.microrregiao.mesorregiao.UF.nome": "mesorregiao_uf_nome",
        "municipio.microrregiao.mesorregiao.UF.regiao.nome": "mesorregiao_uf_regiao_nome",
        "municipio.regiao-imediata.nome": "regiao_imediata",
        "municipio.regiao-imediata.regiao-intermediaria.nome": "regiao_intermediaria",
        "municipio.regiao-imediata.regiao-intermediaria.UF.sigla": "regiao_intermediaria_uf",
        "municipio.regiao-imediata.regiao-intermediaria.UF.nome": "regiao_intermediaria_uf_nome",
        "municipio.regiao-imediata.regiao-intermediaria.UF.regiao.nome": "regiao_intermediaria_uf_regiao_nome",
    }

    df = result[columns].rename(columns=columns_rename)

    df.drop_duplicates(subset=["geocode"], inplace=True)

    columns_df = df.columns.tolist()
    columns_df.insert(0, "country")

    df["country"] = country

    if log:
        logging.info(f"IBGE file not found at '{ibge_file_path}'...")
        logging.info(f"Creating IBGE file at '{ibge_file_path}'...")

    df.to_csv(ibge_file_path, index=False, columns=columns_df)
