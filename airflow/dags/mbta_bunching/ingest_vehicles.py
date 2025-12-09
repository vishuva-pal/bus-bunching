from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any

import requests

from .config import (
    MBTA_API_BASE_URL,
    MBTA_API_KEY,
    BRONZE_VEHICLES_DIR,
)
from .pipeline_io import _ensure_dir, _clean_dir


def build_vehicles_url(
    routes: Optional[List[str]] = None,
    route_type: Optional[int] = 3,
) -> str:
    """
    Build MBTA /vehicles endpoint URL.

    - route_type=3 => buses
    - routes=None or [] => no route filter (all bus routes)
    """
    base = f"{MBTA_API_BASE_URL}/vehicles"
    params: List[str] = []

    if route_type is not None:
        params.append(f"filter[route_type]={route_type}")

    if routes:
        routes_param = ",".join(routes)
        params.append(f"filter[route]={routes_param}")

    if params:
        return base + "?" + "&".join(params)
    return base


def fetch_vehicles(routes: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Fetch vehicle data (buses by default) for the given routes.
    If routes is None/empty, fetch all buses.
    """
    url = build_vehicles_url(routes)

    print("Requesting:", url)

    headers: Dict[str, str] = {}
    if MBTA_API_KEY:
        headers["x-api-key"] = MBTA_API_KEY

    response = requests.get(url, headers=headers, timeout=10)
    response.raise_for_status()
    payload: Dict[str, Any] = response.json()

    print(f"Received {len(payload.get('data', []))} vehicles")
    return payload


def save_snapshot(payload: Dict[str, Any], routes_label: str) -> str:
    """
    Save raw MBTA /vehicles payload as a Bronze snapshot.

    - routes_label is a string used in the filename, e.g. 'all-bus-routes' or '1-15-28'
    - Before writing, clear any older JSON files so only the latest remains.
    """
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    fname = f"vehicles_routes-{routes_label}_{ts}.json"

    out_dir = Path(BRONZE_VEHICLES_DIR)
    _ensure_dir(out_dir)

    _clean_dir(out_dir, "*.json")

    out_path = out_dir / fname
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f)

    return str(out_path)


def run_ingestion(routes: Optional[List[str]] = None, **_: Any) -> str:
    """
    Entry-point used by Airflow's PythonOperator.

    - routes=None => all bus routes
    Returns the path of the snapshot file written to Bronze.
    """
    payload = fetch_vehicles(routes)

    if routes:
        # e.g. ['1', '15', '28'] -> '1-15-28'
        label = "-".join(sorted(routes))
    else:
        label = "all-bus-routes"

    path = save_snapshot(payload, label)
    print(f"Saved snapshot for routes {routes or 'ALL BUS ROUTES'} to {path}")
    return path

if __name__ == "__main__":
    run_ingestion()