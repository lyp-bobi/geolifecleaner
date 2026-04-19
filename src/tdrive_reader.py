"""
T-Drive Taxi Trajectories dataset reader.

Expected input structure (zip or extracted directory):
  taxi_log_2008_by_id/
    {taxi_id}.txt

File format (no header):
  taxi_id, datetime, longitude, latitude
  e.g.: 1,2008-02-02 15:36:08,116.51172,39.92123
"""

import zipfile
import re
from io import TextIOWrapper
from pathlib import Path

import pandas as pd


_TDRIVE_COLS = ["taxi_id", "timestamp", "longitude", "latitude"]
_TDRIVE_DTYPES = {
    "taxi_id": "int64",
    "longitude": "float64",
    "latitude": "float64",
}


def _parse_tdrive_file(fileobj, taxi_id: int) -> pd.DataFrame:
    df = pd.read_csv(
        fileobj,
        header=None,
        names=_TDRIVE_COLS,
        dtype={**_TDRIVE_DTYPES, "timestamp": str},
        on_bad_lines="skip",
    )
    df["timestamp"] = pd.to_datetime(df["timestamp"], format="%Y-%m-%d %H:%M:%S", errors="coerce")
    df["taxi_id"] = taxi_id
    return df


def _iter_zip(zip_path: Path, user_ids: list | None):
    file_pattern = re.compile(r"(\d+)\.txt$", re.IGNORECASE)

    with zipfile.ZipFile(zip_path) as zf:
        for name in zf.namelist():
            m = file_pattern.search(name)
            if not m:
                continue
            tid = int(m.group(1))
            if user_ids is not None and tid not in user_ids:
                continue
            with zf.open(name) as f:
                yield tid, _parse_tdrive_file(TextIOWrapper(f, encoding="utf-8"), tid)


def _iter_dir(base: Path, user_ids: list | None):
    # Support nested structure: base/taxi_log_2008_by_id/*.txt or base/*.txt
    txt_dir = next(base.rglob("taxi_log_2008_by_id"), None)
    search_dir = txt_dir if txt_dir and txt_dir.is_dir() else base

    for txt_file in sorted(search_dir.glob("*.txt")):
        try:
            tid = int(txt_file.stem)
        except ValueError:
            continue
        if user_ids is not None and tid not in user_ids:
            continue
        with open(txt_file, encoding="utf-8") as f:
            yield tid, _parse_tdrive_file(f, tid)


def read_tdrive(
    path: str | Path,
    user_ids: list | None = None,
) -> dict[int, pd.DataFrame]:
    """
    Read T-Drive dataset from a zip file or extracted directory.

    Returns a dict mapping taxi_id -> DataFrame with columns:
      taxi_id, timestamp, longitude, latitude
    """
    path = Path(path)
    user_ids_int = [int(u) for u in user_ids] if user_ids is not None else None

    if path.is_file() and path.suffix.lower() == ".zip":
        iterator = _iter_zip(path, user_ids_int)
    elif path.is_dir():
        iterator = _iter_dir(path, user_ids_int)
    else:
        raise FileNotFoundError(f"T-Drive path not found or unsupported format: {path}")

    return {tid: df for tid, df in iterator}
