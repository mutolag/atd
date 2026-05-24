#!/usr/bin/env python3
"""
Модуль Г: batch-агрегация 30 мин, ELT Silver → Gold ClickHouse.
Учёт опоздавших данных через watermark.
"""
import os
from datetime import datetime, timedelta, timezone

import pandas as pd
import psycopg2
from clickhouse_driver import Client

PG_DSN = (
    f"host={os.getenv('PG_HOST','localhost')} port={os.getenv('PG_PORT','5432')} "
    f"dbname={os.getenv('PG_DB','silver')} user={os.getenv('PG_USER','postgres')} "
    f"password={os.getenv('PG_PASSWORD','postgres')}"
)
CH = Client(
    host=os.getenv("CH_HOST", "localhost"),
    port=int(os.getenv("CH_PORT", "9000")),
    user=os.getenv("CH_USER", "default"),
    password=os.getenv("CH_PASSWORD", "user"),
    database=os.getenv("CH_DATABASE", "transport"),
)
WINDOW_MIN = 30
LATE_GRACE_MIN = 15


def get_watermark():
    rows = CH.execute(
        "SELECT last_event_time FROM gold_watermark WHERE pipeline_name = %(p)s",
        {"p": "silver_to_gold_batch"},
    )
    if rows:
        return rows[0][0]
    return datetime(1970, 1, 1)


def set_watermark(ts):
    CH.execute(
        "ALTER TABLE gold_watermark DELETE WHERE pipeline_name = %(p)s",
        {"p": "silver_to_gold_batch"},
    )
    CH.execute(
        "INSERT INTO gold_watermark (pipeline_name, last_event_time) VALUES",
        [("silver_to_gold_batch", ts)],
    )


def run_batch(**_context):
    wm = get_watermark()
    # Пересчёт с учётом опоздавших: окно [wm - grace, now]
    start = wm - timedelta(minutes=LATE_GRACE_MIN)
    end = datetime.now(timezone.utc)
    query = """
        SELECT camera_id, direction, vehicle_type_id, event_time, speed_kmh
        FROM silver_detections
        WHERE event_time > %s AND event_time <= %s AND is_anomaly = false
    """
    with psycopg2.connect(PG_DSN) as conn:
        df = pd.read_sql(query, conn, params=(start, end))
    if df.empty:
        print("No new silver rows")
        return

    df["event_time"] = pd.to_datetime(df["event_time"])
    type_map = {2: "car", 3: "motorcycle", 5: "bus", 7: "truck"}
    df["vehicle_type"] = df["vehicle_type_id"].map(type_map).fillna("unknown")
    df["window_start"] = df["event_time"].dt.floor(f"{WINDOW_MIN}min")

    agg = (
        df.groupby(["camera_id", "direction", "window_start", "vehicle_type"])
        .agg(
            total_count=("speed_kmh", "count"),
            avg_speed_kmh=("speed_kmh", "mean"),
            p25_speed=("speed_kmh", lambda s: s.quantile(0.25)),
            p75_speed=("speed_kmh", lambda s: s.quantile(0.75)),
        )
        .reset_index()
    )
    total_by_win = df.groupby(["camera_id", "direction", "window_start"]).size().reset_index(name="win_total")
    agg = agg.merge(total_by_win, on=["camera_id", "direction", "window_start"])
    agg["share_of_total"] = agg["total_count"] / agg["win_total"].clip(lower=1)
    agg["window_end"] = agg["window_start"] + timedelta(minutes=WINDOW_MIN)

    rows = []
    for _, r in agg.iterrows():
        rows.append([
            r["camera_id"],
            r["direction"] or "unknown",
            r["window_start"].to_pydatetime(),
            r["window_end"].to_pydatetime(),
            r["vehicle_type"],
            int(r["total_count"]),
            float(r["avg_speed_kmh"] or 0),
            float(r["p25_speed"] or 0),
            float(r["p75_speed"] or 0),
            float(r["share_of_total"]),
            False,
        ])
    CH.execute(
        """INSERT INTO gold_traffic_aggregates (
            camera_id, direction, window_start, window_end, vehicle_type,
            total_count, avg_speed_kmh, p25_speed, p75_speed, share_of_total, is_anomaly
        ) VALUES""",
        rows,
    )
    set_watermark(end)
    print(f"Batch loaded {len(rows)} aggregate rows, watermark={end}")


if __name__ == "__main__":
    run_batch()
