import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

from config import DB_URL, ACCIDENTS_CSV_PATH, WEATHER_CSV_PATH


def get_engine():
    return create_engine(DB_URL)

def load_weather(engine):
    print("Loading weather data...")

    df = pd.read_csv(WEATHER_CSV_PATH)

    date_col_candidates = ["DATE", "date", "datetime"]
    date_col = None
    for c in date_col_candidates:
        if c in df.columns:
            date_col = c
            break

    if date_col is None:
        date_col = df.columns[0]
        print(f"WARNING: using first column '{date_col}' as date, "
              f"because none of {date_col_candidates} were found.")

    df["date"] = pd.to_datetime(df[date_col], errors="coerce").dt.date
    df = df.dropna(subset=["date"])

    tmax_candidates = ["TMAX", "tmax", "temp_max", "MAX_TEMPERATURE", "max_temp"]
    tmin_candidates = ["TMIN", "tmin", "temp_min", "MIN_TEMPERATURE", "min_temp"]
    prcp_candidates = ["PRCP", "prcp", "precip", "precipitation", "RAIN", "rain"]

    def pick_col(candidates):
        for c in candidates:
            if c in df.columns:
                return c
        return None

    tmax_col = pick_col(tmax_candidates)
    tmin_col = pick_col(tmin_candidates)
    prcp_col = pick_col(prcp_candidates)

    df["temp_max"] = pd.to_numeric(df[tmax_col], errors="coerce") if tmax_col else np.nan
    df["temp_min"] = pd.to_numeric(df[tmin_col], errors="coerce") if tmin_col else np.nan
    df["precipitation"] = pd.to_numeric(df[prcp_col], errors="coerce") if prcp_col else np.nan

    if "conditions" in df.columns:
        df["weather_description"] = df["conditions"].fillna("Unknown").astype(str)
    else:
        df["weather_description"] = "Unknown"

    if "preciptype" in df.columns:
        df["precipitation_type"] = df["preciptype"].fillna("Unknown").astype(str)
    else:
        df["precipitation_type"] = "Unknown"

    weather_df = df[
        [
            "date",
            "weather_description",
            "precipitation",
            "precipitation_type",
            "temp_max",
            "temp_min",
        ]
    ].drop_duplicates(subset=["date"])

    with engine.begin() as conn:
        weather_df.to_sql("weather", conn, if_exists="append", index=False)

    print(f"Inserted {len(weather_df)} weather rows.")

def load_accidents(engine, year_min=None, year_max=None, max_rows=None):
    print("Loading accident data...")

    df = pd.read_csv(ACCIDENTS_CSV_PATH, low_memory=False)

    df.columns = [c.upper().replace(" ", "_") for c in df.columns]

    required_cols = [
        "COLLISION_ID",
        "CRASH_DATE",
        "CRASH_TIME",
        "BOROUGH",
        "ZIP_CODE",
        "LATITUDE",
        "LONGITUDE",
        "ON_STREET_NAME",
        "OFF_STREET_NAME",
        "NUMBER_OF_PERSONS_INJURED",
        "NUMBER_OF_PERSONS_KILLED",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Accidents CSV is missing required columns: {missing}")

    df["CRASH_DATE_PARSED"] = pd.to_datetime(df["CRASH_DATE"], errors="coerce")
    df = df.dropna(subset=["CRASH_DATE_PARSED"])

    if year_min is not None:
        df = df[df["CRASH_DATE_PARSED"].dt.year >= year_min]
    if year_max is not None:
        df = df[df["CRASH_DATE_PARSED"].dt.year <= year_max]

    df["CRASH_TIME"] = df["CRASH_TIME"].astype(str).str.strip()
    df["CRASH_DATETIME"] = pd.to_datetime(
        df["CRASH_DATE_PARSED"].dt.strftime("%Y-%m-%d") + " " + df["CRASH_TIME"],
        errors="coerce",
    )
    df = df.dropna(subset=["CRASH_DATETIME"])

    df["LATITUDE"] = pd.to_numeric(df["LATITUDE"], errors="coerce")
    df["LONGITUDE"] = pd.to_numeric(df["LONGITUDE"], errors="coerce")
    df = df.dropna(subset=["LATITUDE", "LONGITUDE"])

    df["NUMBER_OF_PERSONS_INJURED"] = (
        df["NUMBER_OF_PERSONS_INJURED"].fillna(0).astype(int)
    )
    df["NUMBER_OF_PERSONS_KILLED"] = (
        df["NUMBER_OF_PERSONS_KILLED"].fillna(0).astype(int)
    )
    df["num_injuries"] = df["NUMBER_OF_PERSONS_INJURED"]
    df["num_deaths"] = df["NUMBER_OF_PERSONS_KILLED"]

    df["severity"] = np.where(
        (df["num_deaths"] > 0) | (df["num_injuries"] >= 3), 1, 0
    )

    df["ON_STREET_NAME"] = df["ON_STREET_NAME"].fillna("").astype(str)
    df["OFF_STREET_NAME"] = df["OFF_STREET_NAME"].fillna("").astype(str)

    df["street_name"] = df["ON_STREET_NAME"]
    empty_mask = df["street_name"].str.strip().eq("")
    df.loc[empty_mask, "street_name"] = df.loc[empty_mask, "OFF_STREET_NAME"]
    df["street_name"] = df["street_name"].replace("", "Unknown")

    df["street_type"] = "ON_STREET"

    df["crash_date"] = df["CRASH_DATE_PARSED"].dt.date

    df = df.drop_duplicates(subset=["COLLISION_ID"])

    if max_rows is not None and len(df) > max_rows:
        df = df.sample(n=max_rows, random_state=0)

    with engine.connect() as conn:
        rows = conn.execute(text("SELECT weather_id, date FROM weather")).fetchall()
    date_to_weather = {r.date: r.weather_id for r in rows}

    df["weather_id"] = df["crash_date"].map(date_to_weather)
    df = df[df["weather_id"].notna()]
    df["weather_id"] = df["weather_id"].astype(int)

    accidents_df = df[
        [
            "COLLISION_ID",
            "CRASH_DATETIME",
            "crash_date",
            "BOROUGH",
            "ZIP_CODE",
            "LATITUDE",
            "LONGITUDE",
            "street_name",
            "street_type",
            "num_injuries",
            "num_deaths",
            "severity",
            "weather_id",
        ]
    ].rename(
        columns={
            "COLLISION_ID": "accident_id",
            "CRASH_DATETIME": "crash_datetime",
            "BOROUGH": "borough",
            "ZIP_CODE": "zip_code",
            "LATITUDE": "latitude",
            "LONGITUDE": "longitude",
        }
    )

    accidents_df.to_sql("accidents", engine, if_exists="append", index=False)
    print(f"Inserted {len(accidents_df)} accident rows.")


def main():
    engine = get_engine()
    load_weather(engine)
    load_accidents(engine, max_rows=500000) 


if __name__ == "__main__":
    main()