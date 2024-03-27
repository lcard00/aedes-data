from src.ibge import process_ibge_data
from src.infodengue import prepare_api_request, process_infodengue_data
from src.merge import merge_city, merge_country, merge_uf
from src.utils import set_logging_config
import pandas as pd


set_logging_config()

log = True

check_infodengue = False
merge = False

year = 2024
list_uf = []  # ["MG",] # "ES", "SP"]
list_city = (
    []
)  # ["Divinópolis",] # "Carmo do Cajuru", "Nova Serrana", "Vila Velha", "Vitória", "São Paulo", "Jaboticabal"]


params = {
    "country": "Brasil",
    "ibge_path": "data/Brasil/_ibge",
    "ibge_file_name": "ibge_data.csv",
    "infodengue_file_name": "infodengue_data",
    "year": year,
    "ew_start": 1,
    "ew_end": 53,
    "format": "json",
    "ibge_api": "https://servicodados.ibge.gov.br/api/v1/localidades/distritos",
    "infodengue_api": "https://info.dengue.mat.br/api/alertcity",
    "disease_values": ["dengue", "chikungunya"],  # zika
    "list_city": list_city,
    "list_uf": list_uf,
}

columns = ["mesorregiao_uf", "geocode"]
df_ibge = (
    process_ibge_data(params=params, log=log)[columns]
    .sort_values(by=columns)
    .reset_index(drop=True)
)

params["ibge_data"] = df_ibge

df_infodengue = process_infodengue_data(params=params, log=log)

params["infodengue_data"] = df_infodengue

if check_infodengue:
    prepare_api_request(params=params, log=log)

if merge:
    df_ibge = process_ibge_data(params=params, log=log)
    params["ibge_data"] = df_ibge

    merge_city(params=params, log=log)
    merge_uf(params=params, log=log)
    merge_country(params=params, log=log)

    # path = "data/Brasil/infodengue_data_brasil.parquet"

    # df = pd.read_parquet(path)

# mask_u = df["mesorregiao_uf"] == "MG"
# mask_c = df["municipio"] == "Divinópolis"
# mask_d = df["disease"].isin(["dengue"])#["dengue", "chikungunya"])
# mask_y = df["year"].isin(range(2010, 2025))

# mask = mask_u & mask_d & mask_y
# df_u = df[mask].copy()
# df_u = df_u.groupby(["mesorregiao_uf", "disease", "year"]).casos.agg(["sum"])
# df_u["mean"] = df_u.groupby('year')['sum'].transform(lambda x: round(x / 3 if x.name == 2024 else x / 12, 2))

# print(df_u)

# mask = mask_u & mask_c & mask_d

# df_d = df[mask].copy()
# df_d = df_d.groupby(["municipio", "disease", "year"]).casos.agg(["sum"])
# df_d["mean"] = df_d.groupby('year')['sum'].transform(lambda x: round(x / 3 if x.name == 2024 else x / 12, 2))


# print(df_d)


# print(f"Estados: {df["mesorregiao_uf"].nunique()}")
# print(f"Municipios: {df["municipio"].nunique()}")
