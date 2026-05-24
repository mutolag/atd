#!/usr/bin/env python3
"""
Модуль Д: Prophet — прогноз средней скорости на 30 мин вперёд.
Учёт camera_id + direction. Метрики MAE/RMSE/MAPE.
"""
import os
import warnings
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from clickhouse_driver import Client
from prophet import Prophet
from sklearn.metrics import mean_absolute_error, mean_squared_error

warnings.filterwarnings("ignore")

CH = Client(
    host=os.getenv("CH_HOST", "localhost"),
    port=int(os.getenv("CH_PORT", "9000")),
    user=os.getenv("CH_USER", "default"),
    password=os.getenv("CH_PASSWORD", "user"),
    database=os.getenv("CH_DATABASE", "transport"),
)
HISTORY_HOURS = int(os.getenv("ML_HISTORY_HOURS", "72"))
FORECAST_MIN = 30
MODEL_VERSION = "prophet-v1"


def load_series(camera_id, direction):
    q = """
    SELECT window_start, avg(avg_speed_kmh) AS y
    FROM gold_traffic_aggregates
    WHERE camera_id = %(cam)s AND direction = %(dir)s
      AND window_start >= now() - INTERVAL %(h)s HOUR
    GROUP BY window_start ORDER BY window_start
    """
    rows = CH.execute(q, {"cam": camera_id, "dir": direction, "h": HISTORY_HOURS})
    if len(rows) < 10:
        return None
    df = pd.DataFrame(rows, columns=["ds", "y"])
    df["ds"] = pd.to_datetime(df["ds"])
    return df


def run_forecast_pipeline(**_context):
    keys = CH.execute(
        "SELECT DISTINCT camera_id, direction FROM gold_traffic_aggregates"
    )
    if not keys:
        print("No batch data for ML")
        return

    for camera_id, direction in keys:
        df = load_series(camera_id, direction)
        if df is None:
            continue
        split = df["ds"].max() - timedelta(hours=24)
        train = df[df["ds"] <= split]
        test = df[df["ds"] > split]
        if len(train) < 8:
            continue

        model = Prophet(daily_seasonality=True, weekly_seasonality=False)
        model.fit(train.rename(columns={"y": "y"}))

        mae = rmse = None
        if len(test) >= 3:
            future = model.make_future_dataframe(periods=len(test), freq="30min", include_history=False)
            fc = model.predict(future)
            merged = test.merge(fc[["ds", "yhat"]], on="ds")
            mae = mean_absolute_error(merged["y"], merged["yhat"])
            rmse = float(np.sqrt(mean_squared_error(merged["y"], merged["yhat"])))

        future = model.make_future_dataframe(periods=1, freq="30min", include_history=False)
        pred = model.predict(future).iloc[-1]
        pred_time = datetime.utcnow()
        CH.execute(
            """INSERT INTO gold_predictions (
                camera_id, direction, prediction_time, forecast_horizon_minutes,
                predicted_avg_speed_kmh, prediction_interval_lower, prediction_interval_upper,
                model_version, mae_holdout, rmse_holdout
            ) VALUES""",
            [[
                camera_id,
                direction,
                pred_time,
                FORECAST_MIN,
                float(pred["yhat"]),
                float(pred["yhat_lower"]),
                float(pred["yhat_upper"]),
                MODEL_VERSION,
                float(mae or 0),
                float(rmse or 0),
            ]],
        )
        print(f"Forecast {camera_id}/{direction}: {pred['yhat']:.1f} km/h MAE={mae}")


if __name__ == "__main__":
    run_forecast_pipeline()
