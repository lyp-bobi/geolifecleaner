"""
GeoLife & T-Drive Trajectory Data Cleaner
Entry point: reads config.yaml, processes datasets, writes output.
"""

import copy
import sys
from pathlib import Path

import yaml

from src.geolife_reader import read_geolife, iter_geolife
from src.tdrive_reader import read_tdrive
from src.processor import process
from src.stats import load_or_compute_stats
from src.writer import StreamingWriter


def load_config(path: str = "config.yaml") -> dict:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def resolve_path(p: str) -> Path:
    return (Path(__file__).parent / p).resolve()


def _find_input(path: Path, label: str) -> Path:
    if path.is_dir():
        zips = list(path.glob("*.zip"))
        if zips:
            print(f"[{label}] Found zip: {zips[0].name}")
            return zips[0]
        print(f"[{label}] Reading directory: {path}")
        return path
    print(f"[{label}] Reading: {path}")
    return path


def _offset_seeds(cfg: dict, offset: int) -> dict:
    """Return a deep copy of cfg with every step seed shifted by offset.

    Steps with seed=null are left as null (numpy uses system entropy → each
    call naturally differs).  Steps with an integer seed use seed+offset so
    that replicas are reproducible but distinct.
    """
    cfg = copy.deepcopy(cfg)
    proc = cfg.get("processing", {})
    for step in ("time_shift", "spatial_rotation", "spatial_displacement"):
        step_cfg = proc.get(step, {})
        if isinstance(step_cfg.get("seed"), int):
            step_cfg["seed"] += offset
    return cfg


def run_geolife(cfg: dict):
    in_cfg = cfg["input"]["geolife"]
    out_cfg = cfg["output"]

    path = _find_input(resolve_path(in_cfg["path"]), "GeoLife")
    user_ids = in_cfg.get("user_ids")
    include_labels = in_cfg.get("include_labels", True)

    out_dir = resolve_path(out_cfg["directory"])
    stats_path = out_dir / "geolife_statistics.json"

    dataset_filters = {
        "bbox": in_cfg.get("bbox"),
        "date_range": in_cfg.get("date_range"),
    }
    run_cfg = {**cfg, "filters": dataset_filters}

    replicas = cfg.get("processing", {}).get("replicas", 1)
    prefix = out_cfg.get("geolife_prefix", "geolife")

    # If stats are not yet cached, load everything once to compute them.
    if not stats_path.exists():
        print("[GeoLife] Loading data for statistics (first run only)...")
        data_for_stats = read_geolife(path, user_ids=user_ids, include_labels=include_labels)
        print(f"[GeoLife] Loaded {len(data_for_stats)} users")
        load_or_compute_stats(stats_path, data_for_stats,
                              lat_col="latitude", lon_col="longitude", ts_col="timestamp")
        del data_for_stats
    else:
        load_or_compute_stats(stats_path, {},
                              lat_col="latitude", lon_col="longitude", ts_col="timestamp")

    print(f"[GeoLife] Processing & writing... (replicas={replicas})")

    with StreamingWriter(
        output_dir=out_dir,
        fmt=out_cfg["format"],
        prefix=prefix,
        merge=out_cfg.get("merge", True),
        id_col="user_id",
        ts_col="timestamp",
        lat_col="latitude",
        lon_col="longitude",
        out_cfg=out_cfg,
    ) as sw:
        # Outer loop over users so each user is read from disk only once,
        # even when replicas > 1.
        for uid, df in iter_geolife(path, user_ids=user_ids, include_labels=include_labels):
            for i in range(replicas):
                run_cfg_i = _offset_seeds(run_cfg, i) if replicas > 1 else run_cfg
                key = f"{uid}_r{i}" if replicas > 1 else uid
                result_df = process(df, id_col="user_id", cfg=run_cfg_i,
                                    lat_col="latitude", lon_col="longitude")
                if replicas > 1:
                    result_df = result_df.copy()
                    result_df["user_id"] = key
                n = sw.write_user(key, result_df)
                print(f"  [{key}] {n} segments")

    print(f"[GeoLife] Done. Total segments: {sw.total_segments}")


def run_tdrive(cfg: dict):
    in_cfg = cfg["input"]["tdrive"]
    out_cfg = cfg["output"]

    path = _find_input(resolve_path(in_cfg["path"]), "T-Drive")

    print("[T-Drive] Loading data...")
    data = read_tdrive(path, user_ids=in_cfg.get("user_ids"))
    print(f"[T-Drive] Loaded {len(data)} taxis")

    out_dir = resolve_path(out_cfg["directory"])
    stats_path = out_dir / "tdrive_statistics.json"
    load_or_compute_stats(stats_path, data,
                          lat_col="latitude", lon_col="longitude", ts_col="timestamp")

    dataset_filters = {
        "bbox": in_cfg.get("bbox"),
        "date_range": in_cfg.get("date_range"),
    }
    run_cfg = {**cfg, "filters": dataset_filters}

    replicas = cfg.get("processing", {}).get("replicas", 1)
    prefix = out_cfg.get("tdrive_prefix", "tdrive")
    print(f"[T-Drive] Processing & writing... (replicas={replicas})")

    with StreamingWriter(
        output_dir=out_dir,
        fmt=out_cfg["format"],
        prefix=prefix,
        merge=out_cfg.get("merge", True),
        id_col="taxi_id",
        ts_col="timestamp",
        lat_col="latitude",
        lon_col="longitude",
        out_cfg=out_cfg,
    ) as sw:
        # Outer loop over taxis so each taxi is read from disk only once.
        for tid, df in data.items():
            for i in range(replicas):
                run_cfg_i = _offset_seeds(run_cfg, i) if replicas > 1 else run_cfg
                key = f"{tid}_r{i}" if replicas > 1 else tid
                result_df = process(df, id_col="taxi_id", cfg=run_cfg_i,
                                    lat_col="latitude", lon_col="longitude")
                if replicas > 1:
                    result_df = result_df.copy()
                    result_df["taxi_id"] = key
                n = sw.write_user(key, result_df)
                print(f"  [{key}] {n} segments")

    print(f"[T-Drive] Done. Total segments: {sw.total_segments}")


def main():
    config_path = sys.argv[1] if len(sys.argv) > 1 else "config.yaml"
    cfg = load_config(config_path)
    enabled = cfg["datasets"]["enabled"]
    if "geolife" in enabled:
        run_geolife(cfg)
    if "tdrive" in enabled:
        run_tdrive(cfg)
    print("All done.")


if __name__ == "__main__":
    main()
