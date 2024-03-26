import logging
import os
import pandas as pd
from src.utils import check_dir, check_file, set_csv_path


def merge_city(params, log=False):
    infodengue_file_name = params["infodengue_file_name"]

    df = params["ibge_data"]

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
            file_path = f"{infodengue_file_name}.csv"

            if check_file(city_file) and file[:5] == "aedes":
                list_df.append(pd.read_csv(city_file))

        if list_df:
            df_city = create_df(list_df)

            columns_city = df_city.columns.tolist()
            columns_city.remove("disease")
            columns_city.remove("geocode")

            columns_city = columns_ibge() + columns_city

            df_city["country"] = row["country"]
            df_city["municipio"] = row["municipio"]
            df_city["microrregiao"] = row["microrregiao"]
            df_city["mesorregiao"] = row["mesorregiao"]
            df_city["mesorregiao_uf"] = row["mesorregiao_uf"]
            df_city["mesorregiao_uf_nome"] = row["mesorregiao_uf_nome"]
            df_city["mesorregiao_uf_regiao_nome"] = row["mesorregiao_uf_regiao_nome"]
            df_city["regiao_imediata"] = row["regiao_imediata"]
            df_city["regiao_intermediaria"] = row["regiao_intermediaria"]
            df_city["regiao_intermediaria_uf"] = row["regiao_intermediaria_uf"]
            df_city["regiao_intermediaria_uf_nome"] = row[
                "regiao_intermediaria_uf_nome"
            ]
            df_city["regiao_intermediaria_uf_regiao_nome"] = row[
                "regiao_intermediaria_uf_regiao_nome"
            ]

            df_city.sort_values(by=["disease", "SE"], ascending=[True, False]).to_csv(
                os.path.join(city_path, file_path),
                columns=columns_city,
                index=False,
            )

            if log:
                logging.info(
                    f"{row['municipio']} ({row['mesorregiao_uf']}) - Merging done!"
                )
        else:
            if log:
                logging.info(
                    f"{row['municipio']} ({row['mesorregiao_uf']}) - No data found!"
                )


def merge_uf(params, log=False):
    infodengue_file_name = params["infodengue_file_name"]
    df = params["ibge_data"].sort_values(by=["mesorregiao_uf", "municipio"])

    df = df[["mesorregiao_uf", "geocode"]]
    df["csv_path"] = set_csv_path(params, log=log)
    df["uf_path"] = set_csv_path(params, uf=True, log=log)

    if log:
        logging.info(f"Merging UF data...")

    list_uf = df["mesorregiao_uf"].unique().tolist()

    if list_uf:
        for uf in list_uf:
            if log:
                logging.info(f"Merging UF {uf} data...")

            mask = df["mesorregiao_uf"] == uf
            df_uf = df[mask].reset_index(drop=True)

            uf_path = df_uf["uf_path"][0]
            uf_file = f"{infodengue_file_name}_{uf.lower()}.csv"
            file_name = f"{infodengue_file_name}.csv"

            list_city = []

            for index, row in df_uf.iterrows():
                file_path = os.path.join(row["csv_path"], file_name)

                if check_file(file_path):
                    list_city.append(pd.read_csv(file_path))

            df_city = create_df(list_city).sort_values(
                by=["geocode", "disease", "SE"], ascending=[True, True, False]
            )
            df_city.to_csv(
                os.path.join(uf_path, uf_file),
                index=False,
            )
            if log:
                logging.info(f"Merging UF {uf} data done!")
    else:
        if log:
            logging.info(f"No UF data found!")


def columns_ibge():
    columns = [
        "disease",
        "country",
        "geocode",
        "municipio",
        "microrregiao",
        "mesorregiao",
        "mesorregiao_uf",
        "mesorregiao_uf_nome",
        "mesorregiao_uf_regiao_nome",
        "regiao_imediata",
        "regiao_intermediaria",
        "regiao_intermediaria_uf",
        "regiao_intermediaria_uf_nome",
        "regiao_intermediaria_uf_regiao_nome",
    ]
    return columns


def create_df(list_df):
    return pd.concat(list_df, ignore_index=True)
