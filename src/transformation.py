import os
import json
import logging

import pandas as pd

logger = logging.getLogger(__name__)

AIRFLOW_HOME = "/usr/local/airflow"

COLUMNS = [
    "id", "case_number", "date", "block", "iucr",
    "primary_type", "description", "location_description",
    "arrest", "domestic", "beat", "district", "ward",
    "community_area", "fbi_code", "x_coordinate", "y_coordinate",
    "year", "updated_on", "latitude", "longitude",
]


def transform_chicago_crime(
    file_path: str,
    output_dir: str = os.path.join(AIRFLOW_HOME, "include/data/transformed"),
) -> str:
    # Load raw JSON
    with open(file_path, "r", encoding="utf-8") as f:
        df = pd.DataFrame(json.load(f))
    logger.info("Loaded %d rows", len(df))

    # Drop Socrata metadata columns (:@computed_region_*)
    computed_cols = [c for c in df.columns if c.startswith(":@computed_region")]
    df = df.drop(columns=computed_cols)

    # Fill latitude/longitude from nested location dict when missing
    if "location" in df.columns:
        missing_coords = df["latitude"].isna() | df["longitude"].isna()
        df.loc[missing_coords, "latitude"] = df.loc[missing_coords, "location"].apply(
            lambda loc: loc.get("latitude") if isinstance(loc, dict) else None
        )
        df.loc[missing_coords, "longitude"] = df.loc[missing_coords, "location"].apply(
            lambda loc: loc.get("longitude") if isinstance(loc, dict) else None
        )
        df = df.drop(columns=["location"])

    # Keep only useful columns
    df = df[[c for c in COLUMNS if c in df.columns]]

    # Parse dates
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["updated_on"] = pd.to_datetime(df["updated_on"], errors="coerce")

    # Cast types
    df["id"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["district"] = pd.to_numeric(df["district"], errors="coerce").astype("Int64")
    df["ward"] = pd.to_numeric(df["ward"], errors="coerce").astype("Int64")
    df["community_area"] = pd.to_numeric(df["community_area"], errors="coerce").astype("Int64")
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df["x_coordinate"] = pd.to_numeric(df["x_coordinate"], errors="coerce")
    df["y_coordinate"] = pd.to_numeric(df["y_coordinate"], errors="coerce")
    df["arrest"] = df["arrest"].fillna(False).astype(bool)
    df["domestic"] = df["domestic"].fillna(False).astype(bool)

    # Remove duplicates on primary key
    before = len(df)
    df = df.drop_duplicates(subset=["id"], keep="first")
    if len(df) < before:
        logger.warning("Removed %d duplicate(s)", before - len(df))

    logger.info("Transformation done: %d rows, %d columns", len(df), len(df.columns))

    # Save as Parquet
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "transformed_crimes.parquet")
    df.to_parquet(output_path, index=False)
    logger.info("Saved to %s", output_path)

    return output_path


def run_transform_chicago_crime(**context) -> str:
    file_path = context["ti"].xcom_pull(task_ids="fetch_chicago_crime_data")
    return transform_chicago_crime(file_path=file_path)
