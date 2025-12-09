from __future__ import annotations

from pathlib import Path
import shutil
import sys
from datetime import datetime

def _latest(pattern: str, src_dir: Path) -> Path:
    files = sorted(src_dir.glob(pattern))
    if not files:
        raise FileNotFoundError(f"No files for {pattern} in {src_dir}")
    return files[-1]


def main() -> None:
    # This file is at: airflow/dags/scripts/sync_site_data.py
    # parents[0] = scripts
    # parents[1] = dags
    # parents[2] = airflow
    # parents[3] = bus-bunching (repo root)
    this_file = Path(__file__).resolve()
    repo_root = this_file.parents[3]

    print(f"=== Debug ===")
    print(f"__file__      : {this_file}")
    print(f"repo_root     : {repo_root}")

    gold_scores_dir = repo_root / "airflow" / "dags" / "data" / "gold" / "headway_scores"
    silver_dir = repo_root / "airflow" / "dags" / "data" / "silver" / "vehicles"

    site_data_dir = repo_root / "airflow" / "dags" / "site" / "data"
    site_data_dir.mkdir(parents=True, exist_ok=True)

    print(f"gold_scores_dir exists: {gold_scores_dir.exists()} -> {gold_scores_dir}")
    print(f"silver_dir exists     : {silver_dir.exists()} -> {silver_dir}")
    print(f"site_data_dir         : {site_data_dir} (created if missing)")
    print()

    # Show what source files we see
    print("Headway score files:")
    for f in gold_scores_dir.glob("headway_scores_*.csv"):
        print(f"  - {f.name} (size={f.stat().st_size} bytes)")
    print()

    print("Vehicle silver files:")
    for f in silver_dir.glob("vehicles_*.csv"):
        print(f"  - {f.name} (size={f.stat().st_size} bytes)")
    print()

    # Pick latest and copy
    scores_src = _latest("headway_scores_*.csv", gold_scores_dir)
    vehicles_src = _latest("vehicles_*.csv", silver_dir)

    scores_dest = site_data_dir / "headway_scores_latest.csv"
    vehicles_dest = site_data_dir / "vehicles_latest.csv"

    shutil.copy2(scores_src, scores_dest)
    shutil.copy2(vehicles_src, vehicles_dest)

    print("=== Copied ===")
    print(f"  scores   -> {scores_dest} (size={scores_dest.stat().st_size} bytes)")
    print(f"  vehicles -> {vehicles_dest} (size={vehicles_dest.stat().st_size} bytes)")

    timestamp_path = site_data_dir / "last_updated.txt"
    timestamp_path.write_text(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"))
    print(f"Updated timestamp → {timestamp_path}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERROR:", e, file=sys.stderr)
        raise