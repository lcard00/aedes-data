import logging
import numpy as np
import pandas as pd
from src.utils import get_url_resp, http_response, check_file, check_dir, set_csv_path


def process_infodengue_data(params, log=False):
    df = df_assign(params, log=log)

    params["ibge_data"] = df

    df["csv_path"] = set_csv_path(params, log=log)
    df.drop_duplicates(subset=["geocode", "disease"])
    
    return df


def df_assign(params, log=False):
    df = params["ibge_data"]

    disease_values = params["disease_values"]

    if log:
        logging.info(f"Assigning disease values {disease_values}...")
        
    return pd.concat(
        [df.assign(disease=disease) for disease in disease_values], ignore_index=True
    ).sort_values(by=["geocode", "disease"])


def prepare_api_request(params, log=False):
    year = params["year"]
    df = params["infodengue_data"]
    ew_start = params["ew_start"]
    ew_end = params["ew_end"]
    format = params["format"]
    url = params["infodengue_api"]
    
    columns_sort = ["disease", "SE"]
    columns_ascending = [True, False]
    
    date = pd.Timestamp.today()
    df_mew = pd.DataFrame()
    
    weekofyear = check_weekofyear(date, year, log=log)

    params_request = {
        "year": year,
        "format": format,
        "ew_start": ew_start,
        "ew_end": ew_end,
        "url": url,
        "columns_sort": columns_sort,
        "columns_ascending": columns_ascending,
        "weekofyear": weekofyear,
    }
    
    if log:
        logging.info(f"Preparing API request...")
        logging.info(f"Request parameters: {params_request}")
    
    for index, row in df.iterrows():
        df = pd.DataFrame()
        csv_path = row["csv_path"]
        geocode = row["geocode"]
        disease = row["disease"]

        file_name = "/aedes_data_" + str(year) + ".csv"

        file_path = csv_path + file_name
        check_dir(dir_path=csv_path, log=log)
        
        params_request["geocode"] = geocode
        params_request["disease"] = disease
        params_request["file_path"] = file_path
        params_request["df"] = df
        
        if not check_file(file_path):
            initial_file(params_request, log=log)
        else:
            df = pd.read_csv(file_path)
            params_request["df"] = df
            if log:
                logging.info(f"File '{file_path}' loaded successfully...")

            df_mew = missing_ew_request(
                df=df,
                geocode=geocode,
                disease=disease,
                ew_start=ew_start,
                ew_end=ew_end,
                log=log,
            )

            if not df_mew.empty:
                if log:
                    logging.info("Requesting data for missing EWs...")
                params["df_mew"] = df_mew

                request_api_mew(params_request, log=log)
            else:
                if log:
                    logging.info("No EWs to request...")

            dynamic_request(params_request, log=log)
    return


def check_weekofyear(date, year, log=False):
    if date.year > year:
        if log:
            logging.info(f"Year {date.year} is greater than request year {year}...")
            logging.info(f"Setting week of year to 53...")
        return 53
    else:
        if log:
            logging.info(f"Setting week of year to {date.weekofyear}...")
        return date.weekofyear


def initial_file(params, log=False):
    params["url_resp"] = get_url_resp(
        url=params["url"],
        disease=params["disease"],
        geocode=params["geocode"],
        format=params["format"],
        ew_start=params["ew_start"],
        ew_end=params["ew_end"],
        year=params["year"],
    )

    request_api_data(params, log=log)
    return


def missing_ew_request(df, geocode, disease, ew_start, ew_end, log=False):
    if log:
        logging.info(f"Processing CSV file for missing EW request...")

    mask_ew = (
        (df["geocode"] == geocode) & (df["disease"] == disease) & (df["SE"] >= ew_start)
    )

    df = df.loc[mask_ew, ["geocode", "disease", "SE"]].copy()
    df_mew = pd.DataFrame()

    if not df.empty:
        df["SE"] = df.loc[:, ["SE"]] % 100

        max_se = df["SE"].max()
        if max_se == ew_end:
            return df_mew

        list_ew = np.arange(ew_start, max_se + 1)
        mask_se = ~np.isin(list_ew, df["SE"])
        missing_ew = list_ew[mask_se]

        df_mew = pd.DataFrame(
            {
                "geocode": geocode,
                "disease": disease,
                "max_se": max_se,
                "missing_ew": missing_ew,
            }
        )
    return df_mew


def request_api_mew(params, log=False):
    df = params["df_mew"]

    for index, row in df.iterrows():
        df = pd.read_csv(params["file_path"])

        params["df"] = df
        params["url_resp"] = get_url_resp(
            url=params["url"],
            disease=row["disease"],
            geocode=row["geocode"],
            format=params["format"],
            ew_start=row["missing_ew"],
            ew_end=row["missing_ew"],
            year=params["year"],
        )

        request_api_data(params, log=log)
        if log:
            logging.info(
                f"[{row['disease']}] - Requesting data for EW {row['missing_ew']} {params['url_resp']}..."
            )

    return


def request_api_data(params, log=False):
    disease = params["disease"]
    geocode = params["geocode"]
    url_resp = params["url_resp"]
    format = params["format"]
    df = params["df"]
    file_path = params["file_path"]
    columns_sort = params["columns_sort"]
    columns_ascending = params["columns_ascending"]

    df_api = http_response(url_resp, format=format, max_retries=3, backoff_factor=60, log=log)

    if df_api.empty:
        if log:
            logging.info(f"Geocoding {geocode} for {disease} has no new data...")
        return
    else:
        if log:
            logging.info(f"Geocoding {geocode} for {disease} has new data...")

    columns_api = df_api.columns.tolist()
    columns_api.insert(0, "disease")
    columns_api.insert(1, "geocode")

    df_api["disease"] = disease
    df_api["geocode"] = geocode

    if log:
        logging.info(f"Processing file '{file_path}'...")

    pd.concat([df, df_api[columns_api]], ignore_index=True).sort_values(
        columns_sort, ascending=columns_ascending
    ).dropna(axis=1).to_csv(file_path, index=False)
    
    if log:
        logging.info(f"File '{file_path}' updated successfully...")
    return True


def dynamic_request(params, log=False):

    df = pd.read_csv(params["file_path"])
    ew_start = params["ew_start"]
    weekofyear = params["weekofyear"] - 1
    disease = params["disease"]
    geocode = params["geocode"]

    check_disease = np.isin([disease], df["disease"])

    if not check_disease:
        params["url_resp"] = get_url_resp(
            url=params["url"],
            disease=disease,
            geocode=params["geocode"],
            format=params["format"],
            ew_start=ew_start,
            ew_end=weekofyear,
            year=params["year"],
        )
        request_api_data(params, log=log)
    else:
        df = df.groupby(["geocode", "disease"])["SE"].max().reset_index()
        df["SE"] = df["SE"] % 100

        mask_geocode_disease_weekofyear = (
            (df["geocode"] == params["geocode"])
            & (df["SE"] < weekofyear)
            & (df["disease"] == params["disease"])
        )

        if log:
            logging.info(f"Processing CSV file for dynamic request...")

        df = df[mask_geocode_disease_weekofyear]

        if not df.empty:
            df["SE"] += 1
            ew_start = df["SE"].values[0]

            if log:
                logging.info(f"New start of EW for request: {ew_start}")

            params["url_resp"] = get_url_resp(
                url=params["url"],
                disease=disease,
                geocode=params["geocode"],
                format=params["format"],
                ew_start=ew_start,
                ew_end=weekofyear,
                year=params["year"],
            )
            if log:
                logging.info(
                    f"[{disease}] - Requesting dynamic data from the API... {params['url_resp']}..."
                )

            request_api_data(params, log=log)
        else:
            if log:
                logging.info(
                    f"[{disease}] - No dynamic data to request for geocoding {geocode}..."
                )
    return
