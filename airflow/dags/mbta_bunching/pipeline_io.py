from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from .config import (
    BRONZE_VEHICLES_DIR,
    SILVER_VEHICLES_DIR,
    GOLD_GAPS_DIR,
    GOLD_SCORES_DIR,
)
from .compute_headways import compute_headways_for_snapshot

BRONZE_DIR = Path(BRONZE_VEHICLES_DIR)
SILVER_DIR = Path(SILVER_VEHICLES_DIR)
GAPS_DIR = Path(GOLD_GAPS_DIR)
SCORES_DIR = Path(GOLD_SCORES_DIR)


def _ensure_dir(path: Path) -> None:
    """Create directory (and parents) if it does not exist."""
    path.mkdir(parents=True, exist_ok=True)


def _clean_dir(path: Path, pattern: str) -> None:
    """
    Delete all files matching pattern in the given directory.

    Examples
    --------
    _clean_dir(SILVER_DIR, "*.csv")
    _clean_dir(GAPS_DIR, "*.csv")
    """
    path = Path(path)
    if not path.exists():
        return
    for f in path.glob(pattern):
        try:
            f.unlink()
        except OSError:
            pass


def _latest_file(directory: Path, suffix: str) -> Path:
    """Return the latest file (by name sort) with the given suffix in directory."""
    _ensure_dir(directory)
    files = sorted(directory.glob(f"*{suffix}"))
    if not files:
        raise FileNotFoundError(f"No *{suffix} files found in {directory}")
    return files[-1]


def transform_latest_snapshot_to_silver(**_: Any) -> str:
    """
    Read latest raw JSON snapshot from Bronze and write a flat vehicles CSV to Silver.
    Before writing, clear any existing Silver CSVs so only the latest remains.

    Returns
    -------
    str
        Path to the Silver CSV.
    """
    _ensure_dir(BRONZE_DIR)
    _ensure_dir(SILVER_DIR)

    latest_json = _latest_file(BRONZE_DIR, ".json")

    with latest_json.open("r", encoding="utf-8") as f:
        payload = json.load(f)

    rows = []
    for item in payload.get("data", []):
        attrs = item.get("attributes", {})
        rels = item.get("relationships", {})

        route_rel = (rels.get("route") or {}).get("data", {}) or {}
        trip_rel = (rels.get("trip") or {}).get("data", {}) or {}
        stop_rel = (rels.get("stop") or {}).get("data") or {}

        row = {
            "vehicle_id": item.get("id"),
            "route_id": route_rel.get("id"),
            "trip_id": trip_rel.get("id"),
            "stop_id": stop_rel.get("id"),
            "direction_id": attrs.get("direction_id"),
            "current_status": attrs.get("current_status"),
            "current_stop_sequence": attrs.get("current_stop_sequence"),
            "label": attrs.get("label"),
            "latitude": attrs.get("latitude"),
            "longitude": attrs.get("longitude"),
            "speed": attrs.get("speed"),
            "bearing": attrs.get("bearing"),
            "updated_at": attrs.get("updated_at"),
        }
        rows.append(row)

    df = pd.DataFrame(rows)

    if not df.empty:
        df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce", utc=True)

    tag = latest_json.stem.split("_")[-1]

    _clean_dir(SILVER_DIR, "*.csv")

    silver_path = SILVER_DIR / f"vehicles_{tag}.csv"
    df.to_csv(silver_path, index=False)

    print(f"[Silver] Wrote {len(df)} rows to {silver_path}")
    return str(silver_path)


def compute_gold_from_latest_silver(**_: Any) -> str:
    """
    Read latest Silver vehicles CSV and compute headway gaps + scores into Gold.

    Before writing, clear any existing Gold CSVs so that only the latest gaps/scores
    remain in their directories.

    Returns
    -------
    str
        Path to the scores CSV.
    """
    _ensure_dir(SILVER_DIR)
    _ensure_dir(GAPS_DIR)
    _ensure_dir(SCORES_DIR)

    latest_silver = _latest_file(SILVER_DIR, ".csv")

    _clean_dir(GAPS_DIR, "*.csv")
    _clean_dir(SCORES_DIR, "*.csv")

    gaps_path, scores_path = compute_headways_for_snapshot(latest_silver)

    print(f"[Gold] Wrote gaps to {gaps_path}")
    print(f"[Gold] Wrote scores to {scores_path}")
    return str(scores_path)
