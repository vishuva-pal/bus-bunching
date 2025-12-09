from __future__ import annotations

from pathlib import Path
from typing import Tuple, Optional

import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[2] 
GOLD_SCORES_DIR = BASE_DIR / "dags" / "data" / "gold" / "headway_scores"

def _latest_scores_file(scores_dir: Path) -> Path:
    scores_dir.mkdir(parents=True, exist_ok=True)
    files = sorted(scores_dir.glob("headway_scores_*.csv"))
    if not files:
        raise FileNotFoundError(f"No headway_scores_*.csv found in {scores_dir}")
    return files[-1]


def classify_health(score: float) -> str:
    """
    Very simple interpretation of headway_health_score:
      - lower is better (closer to expected headway, less variability).
    You can tune thresholds later.
    """
    if score < 0.002:
        return "🟢 Very healthy spacing (little bunching)"
    if score < 0.004:
        return "🟡 Mild bunching / moderate irregularity"
    if score < 0.008:
        return "🟠 Noticeable bunching – expect lumpy arrivals"
    return "🔴 Severe bunching – long gaps and clumps likely"


def get_route_direction_stats(
    route_id: str,
    direction_id: int,
    scores_path: Path,
) -> Optional[Tuple[pd.Series, str]]:
    """
    Look up the row in headway_scores for (route_id, direction_id).
    Returns (row, health_label) or None if not found.
    """
    scores = pd.read_csv(scores_path)

    scores["route_id_str"] = scores["route_id"].astype(str)

    subset = scores[
        (scores["route_id_str"] == str(route_id))
        & (scores["direction_id"] == float(direction_id))
    ]

    if subset.empty:
        return None

    row = subset.iloc[0]
    label = classify_health(row["headway_health_score"])
    return row, label


def main() -> None:
    print("=== Rider Headway Health Checker ===\n")

    route_id = input("Enter route_id (e.g. 39): ").strip()
    dir_raw = input("Enter direction_id (0 or 1): ").strip()

    try:
        direction_id = int(dir_raw)
    except ValueError:
        print("direction_id must be 0 or 1.")
        return

    origin = input("Enter origin stop_id (optional, press Enter to skip): ").strip()
    dest = input("Enter destination stop_id (optional, press Enter to skip): ").strip()

    scores_path = _latest_scores_file(GOLD_SCORES_DIR)
    print(f"\nUsing headway scores file: {scores_path.name}")

    result = get_route_direction_stats(route_id, direction_id, scores_path)

    if result is None:
        print(
            f"\nNo headway scores found for route {route_id}, "
            f"direction {direction_id} in the latest snapshot."
        )
        return

    row, label = result

    print("\n=== Headway Health Summary ===")
    if origin and dest:
        print(f"For your trip on route {route_id} (dir {direction_id}) "
              f"from stop {origin} to {dest}:")
    else:
        print(f"For route {route_id}, direction {direction_id}:")

    print(f"- Expected headway (schedule): ~{row['expected_headway_min']} minutes")
    print(f"- Median observed headway   : ~{row['median']:.2f} minutes")
    print(f"- Mean observed headway     : ~{row['mean']:.2f} minutes")
    print(f"- Variability (std)         : ~{row['std']:.2f} minutes")
    print(f"- Health score              : {row['headway_health_score']:.6f}")
    print(f"- Interpretation            : {label}")


if __name__ == "__main__":
    main()