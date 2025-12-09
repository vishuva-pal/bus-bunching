from __future__ import annotations

from pathlib import Path
from typing import List, Tuple

import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[1] / "data"
SILVER_DIR = BASE_DIR / "silver" / "vehicles"
GOLD_SCORES_DIR = BASE_DIR / "gold" / "headway_scores"


def _latest_file(directory: Path, suffix: str) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    files = sorted(directory.glob(f"*{suffix}"))
    if not files:
        raise FileNotFoundError(f"No *{suffix} files found in {directory}")
    return files[-1]

def find_candidate_routes(
    silver: pd.DataFrame,
    origin_stop_id: str,
    dest_stop_id: str,
) -> List[Tuple[str, float]]:
    """
    Find (route_id, direction_id) pairs that appear at BOTH origin and dest stops
    in the current Silver snapshot.

    This is a snapshot-based approximation: we look at which routes/directions
    have at least one vehicle currently at origin and at least one at dest.
    """
    silver = silver.copy()
    silver["stop_id"] = silver["stop_id"].astype(str)
    silver["route_id"] = silver["route_id"].astype(str)

    origin = str(origin_stop_id)
    dest = str(dest_stop_id)

    candidates: List[Tuple[str, float]] = []

    grouped = silver.groupby(["route_id", "direction_id"], dropna=False)
    for (route_id, direction_id), group in grouped:
        if group["stop_id"].eq(origin).any() and group["stop_id"].eq(dest).any():
            candidates.append((route_id, float(direction_id) if direction_id is not None else -1.0))

    return candidates


def evaluate_trip_for_routes(
    scores: pd.DataFrame,
    candidates: List[Tuple[str, float]],
) -> pd.DataFrame:
    """
    Lookup headway metrics in the Gold scores table for the candidate routes.
    Returns a DataFrame with one row per (route_id, direction_id).
    """
    if not candidates:
        return pd.DataFrame()

    scores = scores.copy()
    scores["route_id"] = scores["route_id"].astype(str)

    rows = []
    for route_id, direction_id in candidates:
        # direction_id is stored as float in your sample scores
        match = scores[
            (scores["route_id"] == route_id)
            & (scores["direction_id"] == float(direction_id))
        ]
        if not match.empty:
            rows.append(match.iloc[0])

    if not rows:
        return pd.DataFrame()

    return pd.DataFrame(rows)

def format_trip_report(
    origin_stop_id: str,
    dest_stop_id: str,
    result: pd.DataFrame,
) -> str:
    """
    Turn the result rows into a human-readable summary string.
    """
    if result.empty:
        return (
            f"No matching routes found between stops {origin_stop_id} and {dest_stop_id} "
            "in the current snapshot, or no headway scores available yet."
        )

    lines = []
    lines.append("")
    lines.append(f"Trip evaluation for {origin_stop_id} → {dest_stop_id} (current snapshot)")
    lines.append("-" * 72)

    for _, r in result.iterrows():
        route = r["route_id"]
        direction = int(r["direction_id"]) if pd.notna(r["direction_id"]) else -1
        median_gap = r.get("median", None)
        mean_gap = r.get("mean", None)
        std_gap = r.get("std", None)
        count = r.get("count", None)
        expected = r.get("expected_headway_min", None)
        score = r.get("headway_health_score", None)

        bunching_level = "Unknown"
        if pd.notna(score):
            if score >= 0.8:
                bunching_level = "Very regular"
            elif score >= 0.5:
                bunching_level = "Moderately regular"
            elif score >= 0.3:
                bunching_level = "Some bunching"
            else:
                bunching_level = "Heavy bunching / irregular"

        lines.append(f"Route {route} · direction {direction}")
        lines.append(f"  Headway Health Score : {score:.3f}" if pd.notna(score) else "  Headway Health Score : N/A")
        if pd.notna(expected):
            lines.append(f"  Target headway       : {expected:.1f} min")
        if pd.notna(median_gap):
            lines.append(f"  Median gap           : {median_gap:.2f} min")
        if pd.notna(mean_gap):
            lines.append(f"  Mean gap             : {mean_gap:.2f} min")
        if pd.notna(std_gap):
            lines.append(f"  Gap variability (std): {std_gap:.2f} min")
        if pd.notna(count):
            lines.append(f"  Samples used         : {int(count)}")
        lines.append(f"  Bunching level       : {bunching_level}")
        lines.append("")

    return "\n".join(lines)

def evaluate_trip(origin_stop_id: str, dest_stop_id: str) -> None:
    """
    High-level helper you can call from __main__ or elsewhere.

    Uses latest Silver & Gold snapshots to evaluate how bunched the service is
    for routes that appear at both origin and destination stops in the current snapshot.
    """
    silver_path = _latest_file(SILVER_DIR, ".csv")
    scores_path = _latest_file(GOLD_SCORES_DIR, ".csv")

    print("Using Silver file:", silver_path.name)
    print("Using Scores file:", scores_path.name)

    silver = pd.read_csv(silver_path)
    scores = pd.read_csv(scores_path)

    candidates = find_candidate_routes(silver, origin_stop_id, dest_stop_id)
    print("Candidate (route_id, direction_id) pairs:", candidates)

    result = evaluate_trip_for_routes(scores, candidates)
    report = format_trip_report(origin_stop_id, dest_stop_id, result)
    print(report)

if __name__ == "__main__":
    print("=== MBTA Trip Bunching Evaluation (snapshot-based) ===")
    origin = input("Enter origin stop_id: ").strip()
    dest = input("Enter destination stop_id: ").strip()

    if not origin or not dest:
        print("Both origin and destination stop_ids are required.")
    else:
        evaluate_trip(origin, dest)