"""Dataset statistics: compute once, cache to JSON."""

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


def _user_stats(df: pd.DataFrame, lat_col: str, lon_col: str, ts_col: str) -> dict:
    valid_ts = df[ts_col].dropna()
    valid_lat = df[lat_col].dropna()
    valid_lon = df[lon_col].dropna()
    return {
        "point_count": int(len(df)),
        "time_range": [
            valid_ts.min().isoformat() if not valid_ts.empty else None,
            valid_ts.max().isoformat() if not valid_ts.empty else None,
        ],
        "bbox": [
            float(valid_lon.min()) if not valid_lon.empty else None,
            float(valid_lat.min()) if not valid_lat.empty else None,
            float(valid_lon.max()) if not valid_lon.empty else None,
            float(valid_lat.max()) if not valid_lat.empty else None,
        ],
    }


def compute_stats(
    data: dict,
    lat_col: str = "latitude",
    lon_col: str = "longitude",
    ts_col: str = "timestamp",
) -> dict:
    users = {}
    for uid, df in data.items():
        users[str(uid)] = _user_stats(df, lat_col, lon_col, ts_col)

    all_bbox = [
        u["bbox"] for u in users.values()
        if u["bbox"] and all(x is not None for x in u["bbox"])
    ]
    all_tr = sorted(t for u in users.values() for t in u["time_range"] if t is not None)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "user_count": len(users),
        "total_points": sum(u["point_count"] for u in users.values()),
        "bbox": [
            min(b[0] for b in all_bbox),
            min(b[1] for b in all_bbox),
            max(b[2] for b in all_bbox),
            max(b[3] for b in all_bbox),
        ] if all_bbox else None,
        "time_range": [all_tr[0], all_tr[-1]] if all_tr else None,
        "users": users,
    }


def load_or_compute_stats(
    cache_path: Path,
    data: dict,
    lat_col: str = "latitude",
    lon_col: str = "longitude",
    ts_col: str = "timestamp",
) -> dict:
    if cache_path.exists():
        print(f"  [stats] Using cached statistics: {cache_path.name}")
        with open(cache_path, encoding="utf-8") as f:
            return json.load(f)
    print("  [stats] Computing statistics...")
    stats = compute_stats(data, lat_col, lon_col, ts_col)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)
    print(f"  [stats] Saved → {cache_path.name}")
    return stats
