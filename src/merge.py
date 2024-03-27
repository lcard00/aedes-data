import logging
import os
import pandas as pd
from src.utils import check_dir, check_file, set_csv_path


def merge_city(params, log=False):
    infodengue_file_name = params["infodengue_file_name"]

    df = params["ibge_data"].sort_values(by=["mesorregiao_uf", "municipio"])

    df["csv_path"] = set_csv_path(params, log=log)

    for index, row in df.iterrows():
        city_path = row["csv_path"]
        list_df = []
        df_city = pd.DataFrame()

        if log:
            logging.info(
                f"{row['municipio']} ({row['mesorregiao_uf']}) - Merging city data..."
            )

        check_dir(dir_path=city_path, log=log)

        for file in os.listdir(city_path):
            city_file = os.path.join(city_path, file)
            file = f"{infodengue_file_name}.parquet"
            file_path = os.path.join(city_path, file)

            if city_file.endswith(".csv") and check_file(city_file):
                list_df.append(pd.read_csv(city_file))

        if list_df:
            add_columns_and_save(list_df, file_path, row, log=log)

        else:
            if log:
                logging.info(
                    f"{row['municipio']} ({row['mesorregiao_uf']}) - No data found!"
                )


def transform_columns(df, log=False):
    df["tweet"] = 0
    df = drop_columns(df, log=log)

    columns = [
        "casos_est",
        "casos_est_min",
        "casos_est_max",
        "casos",
        "pop",
        "notif_accum_year",
    ]
    if log:
        logging.info(f"Casting columns {columns}...")
    df[columns] = df[columns].fillna(0).astype(int)

    columns = ["p_rt1", "p_inc100k", "Rt", "tempmin", "tempmed", "tempmax"]
    if log:
        logging.info(f"Rounding columns {columns}...")

    df = df.round(
        {
            "p_rt1": 2,
            "p_inc100k": 4,
            "Rt": 2,
            "tempmin": 2,
            "tempmed": 2,
            "tempmax": 2,
            "umidmin": 2,
            "umidmed": 2,
            "umidmax": 2,
        }
    )

    columns = ["year", "week", "receptivo", "transmissao", "nivel_inc"]

    if log:
        logging.info(f"Adding columns {columns}...")

    df["receptivo"] = df["receptivo"].apply(data_receptivo)
    df["transmissao"] = df["transmissao"].fillna(0).apply(data_transmissao)
    df["nivel_inc"] = df["nivel_inc"].apply(data_nivel_inc)

    df["year"] = df["SE"] // 100
    df["SE"] = df["SE"] % 100

    return df


def data_receptivo(value):
    switcher = {
        0: "desfavorável",
        1: "favorável",
        2: "favorável nesta semana e na semana passada",
        3: "favorável por pelo menos três semanas",
    }

    return switcher.get(value, "Invalid")


def data_transmissao(value):
    switcher = {
        0: "nenhuma evidência",
        1: "possível",
        2: "provável",
        3: "altamente provável",
    }

    return switcher.get(value, "Invalid")


def data_nivel_inc(value):
    switcher = {
        0: "Incidência estimada abaixo do limiar pré-epidemia",
        1: "acima do limiar pré-epidemia, mas abaixo do limiar epidêmico",
        2: "acima do limiar epidêmico",
    }

    return switcher.get(value, "Invalid")


def drop_columns(df, log=False):
    columns = ["data_iniSE", "Localidade_id", "id", "tweet", "versao_modelo"]
    if log:
        logging.info(f"Dropping columns {columns}...")

    df.drop(
        columns=columns,
        inplace=True,
    )
    return df


def add_columns_and_save(list_df, file_path, row, log=False):
    df = transform_columns(create_df(list_df), log=log)

    df["country"] = row["country"]
    df["municipio"] = row["municipio"]
    df["microrregiao"] = row["microrregiao"]
    df["mesorregiao"] = row["mesorregiao"]
    df["mesorregiao_uf"] = row["mesorregiao_uf"]
    df["mesorregiao_uf_nome"] = row["mesorregiao_uf_nome"]
    df["mesorregiao_uf_regiao_nome"] = row["mesorregiao_uf_regiao_nome"]
    df["regiao_imediata"] = row["regiao_imediata"]
    df["regiao_intermediaria"] = row["regiao_intermediaria"]
    df["regiao_intermediaria_uf"] = row["regiao_intermediaria_uf"]
    df["regiao_intermediaria_uf_nome"] = row["regiao_intermediaria_uf_nome"]
    df["regiao_intermediaria_uf_regiao_nome"] = row[
        "regiao_intermediaria_uf_regiao_nome"
    ]

    df.sort_values(
        by=["disease", "year", "SE"], ascending=[True, False, False]
    ).to_parquet(
        file_path,
        index=False,
    )

    if log:
        logging.info(f"{row['municipio']} ({row['mesorregiao_uf']}) - Merging done!")


def merge_uf(params, log=False):
    infodengue_file_name = params["infodengue_file_name"]
    df = merge_df(params, uf=True, log=log).sort_values(by=["mesorregiao_uf"])

    if log:
        logging.info(f"Merging UF data...")

    list_uf = df["mesorregiao_uf"].unique().tolist()

    if list_uf:
        for uf in list_uf:
            if log:
                logging.info(f"Merging UF [{uf}] data...")

            mask = df["mesorregiao_uf"] == uf
            df_uf = df[mask].reset_index(drop=True)

            uf_path = df_uf["uf_path"][0]
            uf_file = f"{infodengue_file_name}_{uf.lower()}.parquet"
            file_name = f"{infodengue_file_name}.parquet"

            list_city = []

            for index, row in df_uf.iterrows():
                file_path = os.path.join(row["csv_path"], file_name)

                if check_file(file_path, type="parquet"):
                    list_city.append(pd.read_parquet(file_path))

            df_city = create_df(list_city).sort_values(
                by=["geocode", "disease", "year", "SE"],
                ascending=[True, True, False, False],
            )
            df_city.to_parquet(
                os.path.join(uf_path, uf_file),
                index=False,
            )
            if log:
                logging.info(f"Merging UF {uf} data done!")
    else:
        if log:
            logging.info(f"No UF data found!")


def merge_country(params, log=False):
    infodengue_file_name = params["infodengue_file_name"]
    country = params["country"]
    df = merge_df(params, country=True, uf=True, log=log)
    df = df[["mesorregiao_uf", "uf_path", "country_path"]]

    df.drop_duplicates(inplace=True)

    if log:
        logging.info(f"Merging country data...")

    country_file = f"{infodengue_file_name}_{country.lower()}.parquet"
    country_file_path = os.path.join(df["country_path"].values[0], country_file)

    list_uf = []

    for index, row in df.iterrows():
        uf_file = f"{infodengue_file_name}_{row['mesorregiao_uf'].lower()}.parquet"
        uf_file_path = os.path.join(row["uf_path"], uf_file)

        if check_file(uf_file_path, type="parquet"):
            list_uf.append(pd.read_parquet(uf_file_path))

    if list_uf:
        df_country = create_df(list_uf).sort_values(
            by=["mesorregiao_uf", "geocode", "disease", "year", "SE"],
            ascending=[True, True, True, False, False],
        )

        df_country.to_parquet(country_file_path, index=False)

        if log:
            logging.info(f"Merging country data done!")
    else:
        if log:
            logging.info(f"No country data found!")


def merge_df(params, uf=False, country=False, log=False):
    df = params["ibge_data"].sort_values(by=["mesorregiao_uf", "municipio"])

    df = df[["mesorregiao_uf", "geocode"]]
    df["csv_path"] = set_csv_path(params, log=log)
    if uf:
        df["uf_path"] = set_csv_path(params, uf=True, log=log)
    if country:
        df["country_path"] = set_csv_path(params, country=True, log=log)

    return df


def create_df(list_df):
    return pd.concat(list_df, ignore_index=True)
