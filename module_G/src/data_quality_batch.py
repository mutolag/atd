#!/usr/bin/env python3
"""Batch DQ: аномалии в витринах — лог без удаления данных."""
import os
from datetime import datetime, timedelta
from pathlib import Path

from clickhouse_driver import Client

LOG = Path(os.getenv("LOG_DIR", "logs")) / "batch_quality"
LOG.mkdir(parents=True, exist_ok=True)

CH = Client(
    host=os.getenv("CH_HOST", "localhost"),
    port=int(os.getenv("CH_PORT", "9000")),
    user=os.getenv("CH_USER", "default"),
    password=os.getenv("CH_PASSWORD", "user"),
    database=os.getenv("CH_DATABASE", "transport"),
)

SPEED_MAX = 140
INCIDENTS_MAX_PER_30MIN = 500


def log_issue(camera_id, check_type, detail):
    f = LOG / f"{camera_id or 'global'}.log"
    with open(f, "a", encoding="utf-8") as out:
        out.write(f"{datetime.utcnow().isoformat()}\t{check_type}\t{detail}\n")


def run_checks(**_context):
    since = datetime.utcnow() - timedelta(hours=2)
    rows = CH.execute(
        """
        SELECT camera_id, direction, window_start, avg_speed_kmh, total_count
        FROM gold_traffic_aggregates
        WHERE window_start >= %(since)s
        """,
        {"since": since},
    )
    for cam, direction, ws, avg_speed, cnt in rows:
        if avg_speed and avg_speed > SPEED_MAX:
            log_issue(cam, "batch_speed_anomaly", f"window={ws} speed={avg_speed}")
        if cnt and cnt > 10000:
            log_issue(cam, "batch_count_anomaly", f"window={ws} count={cnt}")

    inc = CH.execute(
        """
        SELECT camera_id, count() FROM gold_incidents
        WHERE timestamp >= %(since)s GROUP BY camera_id
        """,
        {"since": since},
    )
    for cam, c in inc:
        if c > INCIDENTS_MAX_PER_30MIN:
            log_issue(cam, "incident_count_anomaly", f"count={c}")
    print("Batch quality checks done")


if __name__ == "__main__":
    run_checks()
