from __future__ import annotations

from pathlib import Path
from typing import Tuple

import pandas as pd

from .config import (
    SILVER_VEHICLES_DIR,
    GOLD_GAPS_DIR,
    GOLD_SCORES_DIR,
)

SILVER_DIR = Path(SILVER_VEHICLES_DIR)
GAPS_DIR = Path(GOLD_GAPS_DIR)
SCORES_DIR = Path(GOLD_SCORES_DIR)

CONFIG_DIR = Path(__file__).resolve().parent / "data" / "config"
EXPECTED_HEADWAYS_CSV = (
    Path(__file__).resolve().parent
    / "data"
    / "config"
    / "route_expected_headways.csv"
)


def _load_expected_headways() -> pd.DataFrame:
    """
    Load route-level expected headways from CSV.

    Expected schema:
        route_id (str),
        direction_id (int),
        period_name (str),
        expected_headway_min (float)
    """
    if not EXPECTED_HEADWAYS_CSV.exists():
        return pd.DataFrame(
            columns=[
                "route_id",
                "direction_id",
                "period_name",
                "expected_headway_min",
            ]
        )

    df = pd.read_csv(EXPECTED_HEADWAYS_CSV, dtype={"route_id": str})
    df["direction_id"] = pd.to_numeric(df["direction_id"], errors="coerce")
    return df


def compute_headways_for_snapshot(silver_path: str) -> tuple[Path, Path]:
    """
    Given a Silver vehicles CSV, compute:
      - headway gaps (Gold, per stop/bus sequence)
      - headway scores (Gold, per route/direction)

    Returns:
        (gaps_path, scores_path)
    """
    silver_path = Path(silver_path)
    df = pd.read_csv(silver_path)

    df["updated_at"] = pd.to_datetime(df["updated_at"], utc=True, errors="coerce")

    df = df.sort_values(
        ["route_id", "direction_id", "trip_id", "current_stop_sequence", "updated_at"]
    )

    gaps_df = (
        df[["route_id", "direction_id", "updated_at"]]
        .dropna(subset=["route_id", "direction_id", "updated_at"])
        .copy()
    )

    gaps_df["gap_min"] = (
        gaps_df.groupby(["route_id", "direction_id"])["updated_at"]
        .diff()
        .dt.total_seconds()
        / 60.0
    )

    gaps_df = gaps_df.dropna(subset=["gap_min"])

    tag = silver_path.stem.split("_")[-1]
    gaps_path = GAPS_DIR / f"headway_gaps_{tag}.csv"
    GAPS_DIR.mkdir(parents=True, exist_ok=True)
    gaps_df.to_csv(gaps_path, index=False)

    scores_df = (
        gaps_df.groupby(["route_id", "direction_id"])
        .agg(
            median=("gap_min", "median"),
            mean=("gap_min", "mean"),
            std=("gap_min", "std"),
            count=("gap_min", "count"),
        )
        .reset_index()
    )

    scores_df["expected_headway_min"] = 10.0

    scores_df["headway_health_score"] = (
        (scores_df["mean"] - scores_df["expected_headway_min"]).abs()
        + scores_df["std"].fillna(0)
    ) / scores_df["expected_headway_min"]

    scores_path = SCORES_DIR / f"headway_scores_{tag}.csv"
    SCORES_DIR.mkdir(parents=True, exist_ok=True)
    scores_df.to_csv(scores_path, index=False)

    print(f"[Gold] Wrote gaps to {gaps_path}")
    print(f"[Gold] Wrote scores to {scores_path}")
    return gaps_path, scores_path