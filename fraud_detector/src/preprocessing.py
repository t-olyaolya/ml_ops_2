from __future__ import annotations

import logging
from pathlib import Path
from typing import List, Tuple

import numpy as np
import pandas as pd
from category_encoders import CatBoostEncoder
from sklearn.preprocessing import RobustScaler

logger = logging.getLogger(__name__)
RANDOM_STATE = 42

EARTH_RADIUS_M = 6_372_800


def _haversine(
        lat1: pd.Series, lon1: pd.Series, lat2: pd.Series, lon2: pd.Series, *, n_digits: int = 0
) -> pd.Series:
    phi1, phi2 = np.radians(lat1), np.radians(lat2)
    dphi = phi2 - phi1
    dlambda = np.radians(lon2 - lon1)
    a = np.sin(dphi / 2) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(dlambda / 2) ** 2
    dist = 2 * EARTH_RADIUS_M * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    return np.round(dist, n_digits)


def _bearing(
        lat1: pd.Series, lon1: pd.Series, lat2: pd.Series, lon2: pd.Series, *, n_digits: int = 0
) -> pd.Series:
    lat1_rad, lon1_rad = np.radians(lat1), np.radians(lon1)
    lat2_rad, lon2_rad = np.radians(lat2), np.radians(lon2)
    dlon = lon2_rad - lon1_rad
    x = np.sin(dlon) * np.cos(lat2_rad)
    y = np.cos(lat1_rad) * np.sin(lat2_rad) - np.sin(lat1_rad) * np.cos(lat2_rad) * np.cos(dlon)
    return np.round((np.degrees(np.arctan2(x, y)) + 360) % 360, n_digits)


_encoder: CatBoostEncoder | None = None
_scaler: RobustScaler | None = None
_numeric_cols: List[str] | None = None

CAT_COLS: List[str] = ["merch", "cat_id", "gender", "one_city", "us_state", "jobs", "name_1", "name_2", "street"]
TIME_COL = "transaction_time"


def _add_datetime(df: pd.DataFrame) -> pd.DataFrame:
    df[TIME_COL] = pd.to_datetime(df[TIME_COL])
    df["hour"] = df[TIME_COL].dt.hour
    df["dayofweek"] = df[TIME_COL].dt.dayofweek
    df["month"] = df[TIME_COL].dt.month
    return df.drop(columns=[TIME_COL])


def _add_geo(df: pd.DataFrame) -> pd.DataFrame:
    df["hav_dist_1"] = _haversine(df["lat"], df["lon"], df["merchant_lat"], df["merchant_lon"])
    df["hav_dist_2"] = _haversine(df["lat"], df["lon"], 0, 0)
    df["hav_dist_3"] = _haversine(0, 0, df["merchant_lat"], df["merchant_lon"])

    df["bearing_degree_1"] = _bearing(df["lat"], df["lon"], df["merchant_lat"], df["merchant_lon"])
    df["bearing_degree_2"] = _bearing(df["lat"], df["lon"], 0, 0)
    df["bearing_degree_3"] = _bearing(0, 0, df["merchant_lat"], df["merchant_lon"])
    return df


def load_train_data(train_path: str | Path = "./train_data/train.csv") -> pd.DataFrame:
    global _encoder, _scaler, _numeric_cols

    logger.info("Loading training data…")
    train = pd.read_csv(train_path)

    train = _add_datetime(train)
    train = _add_geo(train)

    logger.info("Encoding training data…")
    _encoder = CatBoostEncoder(cols=CAT_COLS, random_state=RANDOM_STATE)
    train_enc = _encoder.fit_transform(train[CAT_COLS], train["target"]).add_suffix("_cb")
    train = pd.concat([train.drop(columns=CAT_COLS), train_enc], axis=1)

    _numeric_cols = [
        "amount", "lat", "lon", "population_city", "merchant_lat", "merchant_lon",
        "hav_dist_1", "hav_dist_2", "hav_dist_3", "bearing_degree_1", "bearing_degree_2", "bearing_degree_3",
        "hour", "dayofweek", "month"]
    logger.info("Scaling training data…")
    _scaler = RobustScaler()
    train[_numeric_cols] = _scaler.fit_transform(train[_numeric_cols])
    train = train.drop(columns=["target"], errors="ignore")
    logger.info("Training data ready – shape %s", train.shape)
    return train


def run_preproc(df: pd.DataFrame) -> pd.DataFrame:
    if _encoder is None or _scaler is None or _numeric_cols is None:
        raise RuntimeError("load_train_data() must be called once before run_preproc().")

    for col in CAT_COLS:
        if col not in df.columns:
            df[col] = np.nan

    df = _add_datetime(df)
    df = _add_geo(df)

    enc_df = _encoder.transform(df[CAT_COLS]).add_suffix("_cb")

    df = pd.concat([df.drop(columns=CAT_COLS), enc_df], axis=1)

    df[_numeric_cols] = _scaler.transform(df[_numeric_cols])
    logger.debug("Preprocessing finished. Shape: %s", df.shape)
    logger.info("Data %s", df.columns)
    return df
