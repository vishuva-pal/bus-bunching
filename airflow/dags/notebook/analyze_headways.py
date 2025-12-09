from pathlib import Path
import json
import pandas as pd

BASE = Path(__file__).resolve().parents[1] / "data"

bronze_dir = BASE / "bronze" / "vehicles_raw"
silver_dir = BASE / "silver" / "vehicles"
gold_scores_dir = BASE / "gold" / "headway_scores"
gold_gaps_dir = BASE / "gold" / "headway_gaps"

bronze_files = sorted(bronze_dir.glob("*.json"))
silver_files = sorted(silver_dir.glob("*.csv"))
scores_files = sorted(gold_scores_dir.glob("*.csv"))
gaps_files = sorted(gold_gaps_dir.glob("*.csv"))

latest_bronze = bronze_files[-1]
latest_silver = silver_files[-1]
latest_scores = scores_files[-1]
latest_gaps = gaps_files[-1]

with open(latest_bronze, "r", encoding="utf-8") as f:
    payload = json.load(f)

df_raw = pd.json_normalize(payload["data"])
print("Raw Bronze shape:", df_raw.shape)

silver = pd.read_csv(latest_silver)
scores = pd.read_csv(latest_scores)
gaps   = pd.read_csv(latest_gaps)
