import logging
import os
import pandas as pd
from src.utils import check_file, set_csv_path


def columns_ibge():
    columns = [
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


def merge_city(params, log=False):
    infodengue_file_name = params["infodengue_file_name"]

    df = params["ibge_data"]

    df["csv_path"] = set_csv_path(params, log=log)

    for index, row in df.iterrows():
        city_path = row["csv_path"]
        list_df = []
        df_city = pd.DataFrame()
        
        if log:
            logging.info(f"{row['municipio']} ({row['mesorregiao_uf']}) - Merging city data...")

        for file in os.listdir(city_path):
            city_file = os.path.join(city_path, file)

            if check_file(city_file):
                list_df.append(pd.read_csv(city_file))

        df_city = create_df(list_df)
        
        columns_city = df_city.columns.tolist()
        
        df_city["country"] = row["country"]
        df_city["municipio"] =  row["municipio"]
        df_city["microrregiao"] = row["microrregiao"]
        df_city["mesorregiao"] = row["mesorregiao"]
        df_city["mesorregiao_uf"] = row["mesorregiao_uf"]
        df_city["mesorregiao_uf_nome"] = row["mesorregiao_uf_nome"]
        df_city["mesorregiao_uf_regiao_nome"] = row["mesorregiao_uf_regiao_nome"]
        df_city["regiao_imediata"] = row["regiao_imediata"]
        df_city["regiao_intermediaria"] = row["regiao_intermediaria"]
        df_city["regiao_intermediaria_uf"] = row["regiao_intermediaria_uf"]
        df_city["regiao_intermediaria_uf_nome"] = row["regiao_intermediaria_uf_nome"]
        df_city["regiao_intermediaria_uf_regiao_nome"] = row["regiao_intermediaria_uf_regiao_nome"]
        
        columns_city = columns_ibge() + columns_city
        
        df_city.sort_values(
            by=["disease", "SE"], ascending=[True, False]
        ).to_csv(os.path.join(city_path, infodengue_file_name), columns=columns_city, index=False)
        
        if log:
            logging.info(f"{row['municipio']} ({row['mesorregiao_uf']}) - Merging done!")


def create_df(list_df):
    return pd.concat(list_df, ignore_index=True)