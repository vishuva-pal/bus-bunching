import os
from typing import List

MBTA_API_BASE_URL = os.getenv("MBTA_API_BASE_URL", "https://api-v3.mbta.com")
MBTA_API_KEY = os.getenv("MBTA_API_KEY")

DEFAULT_ROUTES: List[str] = []
DAGS_DIR = os.getenv("AIRFLOW__CORE__DAGS_FOLDER", "/opt/airflow/dags")
DATA_DIR = os.path.join(DAGS_DIR, "data")

BRONZE_VEHICLES_DIR = os.path.join(DATA_DIR, "bronze", "vehicles_raw")
SILVER_VEHICLES_DIR = os.path.join(DATA_DIR, "silver", "vehicles")
GOLD_GAPS_DIR = os.path.join(DATA_DIR, "gold", "headway_gaps")
GOLD_SCORES_DIR = os.path.join(DATA_DIR, "gold", "headway_scores")

RAW_DATA_DIR = BRONZE_VEHICLES_DIR
