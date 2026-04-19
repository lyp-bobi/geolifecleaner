"""
GeoLife GPS Trajectories dataset reader.

Expected input structure (zip or extracted directory):
  GeoLife GPS Trajectories/
    Data/
      {user_id}/
        Trajectory/
          *.plt
        labels.txt   (optional, transportation mode)

PLT file format (skip first 6 lines):
  Latitude, Longitude, 0, Altitude, Days_since_1899-12-30, Date, Time
"""

import zipfile
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import TextIOWrapper
from pathlib import Path

import numpy as np
import pandas as pd


_PLT_COLS = ["latitude", "longitude", "_zero", "altitude", "_days", "date", "time"]
_PLT_DTYPES = {
    "latitude": "float64",
    "longitude": "float64",
    "altitude": "float64",
}

_LABEL_COLS = ["start_time", "end_time", "transport_mode"]


def _parse_plt(fileobj, user_id: str) -> pd.DataFrame:
    df = pd.read_csv(
        fileobj,
        skiprows=6,
        header=None,
        names=_PLT_COLS,
        dtype=_PLT_DTYPES,
        on_bad_lines="skip",
    )
    df["timestamp"] = pd.to_datetime(
        df["date"] + " " + df["time"], format="%Y-%m-%d %H:%M:%S", errors="coerce"
    )
    df.drop(columns=["_zero", "_days", "date", "time"], inplace=True)
    df.insert(0, "user_id", user_id)
    return df


def _parse_labels(fileobj) -> pd.DataFrame:
    df = pd.read_csv(
        fileobj,
        sep="\t",
        skiprows=1,
        header=None,
        names=_LABEL_COLS,
    )
    df["start_time"] = pd.to_datetime(df["start_time"], format="%Y/%m/%d %H:%M:%S", errors="coerce")
    df["end_time"] = pd.to_datetime(df["end_time"], format="%Y/%m/%d %H:%M:%S", errors="coerce")
    return df


def _attach_labels(traj: pd.DataFrame, labels: pd.DataFrame) -> pd.DataFrame:
    if labels.empty or traj.empty:
        traj["transport_mode"] = pd.NA
        return traj

    traj = traj.sort_values("timestamp").reset_index(drop=True)
    labels = labels.sort_values("start_time").reset_index(drop=True)

    # Use searchsorted for O(m·log n) instead of O(m·n) boolean masking.
    ts = traj["timestamp"].to_numpy()
    modes = np.full(len(traj), pd.NA, dtype=object)
    for _, row in labels.iterrows():
        lo = np.searchsorted(ts, row["start_time"].to_datetime64(), side="left")
        hi = np.searchsorted(ts, row["end_time"].to_datetime64(), side="right")
        if lo < hi:
            modes[lo:hi] = row["transport_mode"]
    traj["transport_mode"] = modes

    return traj


def _iter_zip(zip_path: Path, user_ids: list | None, include_labels: bool):
    user_pattern = re.compile(r"Data/(\d+)/Trajectory/.*\.plt$", re.IGNORECASE)
    label_pattern = re.compile(r"Data/(\d+)/labels\.txt$", re.IGNORECASE)

    with zipfile.ZipFile(zip_path) as zf:
        names = zf.namelist()

        # Build label map
        labels_map: dict[str, pd.DataFrame] = {}
        if include_labels:
            for name in names:
                m = label_pattern.search(name)
                if m:
                    uid = m.group(1)
                    with zf.open(name) as f:
                        labels_map[uid] = _parse_labels(TextIOWrapper(f, encoding="utf-8"))

        # Group PLT files by user, then attach labels once per user
        plt_by_user: dict[str, list[str]] = {}
        for name in names:
            m = user_pattern.search(name)
            if not m:
                continue
            uid = m.group(1)
            if user_ids is not None and uid not in user_ids:
                continue
            plt_by_user.setdefault(uid, []).append(name)

        for uid, plt_names in plt_by_user.items():
            dfs = []
            for name in sorted(plt_names):
                with zf.open(name) as f:
                    dfs.append(_parse_plt(TextIOWrapper(f, encoding="utf-8"), uid))
            if not dfs:
                continue
            user_df = pd.concat(dfs, ignore_index=True)
            if include_labels and uid in labels_map:
                user_df = _attach_labels(user_df, labels_map[uid])
            else:
                user_df["transport_mode"] = pd.NA
            yield uid, user_df


def _read_plt_file(plt_path: Path, uid: str) -> pd.DataFrame:
    with open(plt_path, encoding="utf-8") as f:
        return _parse_plt(f, uid)


def _iter_dir(base: Path, user_ids: list | None, include_labels: bool):
    data_dir = base / "Data" if (base / "Data").exists() else base

    for user_dir in sorted(data_dir.iterdir()):
        if not user_dir.is_dir():
            continue
        uid = user_dir.name
        if user_ids is not None and uid not in user_ids:
            continue

        labels = pd.DataFrame()
        labels_file = user_dir / "labels.txt"
        if include_labels and labels_file.exists():
            with open(labels_file, encoding="utf-8") as f:
                labels = _parse_labels(f)

        traj_dir = user_dir / "Trajectory"
        if not traj_dir.exists():
            continue

        plt_files = sorted(traj_dir.glob("*.plt"))
        if not plt_files:
            continue

        # Parallel reads for users with many PLT files.
        if len(plt_files) > 8:
            with ThreadPoolExecutor() as pool:
                futures = {pool.submit(_read_plt_file, p, uid): p for p in plt_files}
                dfs = [f.result() for f in as_completed(futures)]
        else:
            dfs = [_read_plt_file(p, uid) for p in plt_files]

        user_df = pd.concat(dfs, ignore_index=True)
        if include_labels and not labels.empty:
            user_df = _attach_labels(user_df, labels)
        else:
            user_df["transport_mode"] = pd.NA
        yield uid, user_df


def iter_geolife(
    path: str | Path,
    user_ids: list | None = None,
    include_labels: bool = True,
):
    """Lazy generator yielding (user_id, DataFrame) one user at a time.

    Prefer this over read_geolife when processing users sequentially, since it
    avoids loading the entire dataset into memory at once.
    """
    path = Path(path)
    user_ids_str = [str(u) for u in user_ids] if user_ids is not None else None

    if path.is_file() and path.suffix.lower() == ".zip":
        yield from _iter_zip(path, user_ids_str, include_labels)
    elif path.is_dir():
        candidate = next(path.rglob("Data"), None)
        base = candidate.parent if candidate else path
        yield from _iter_dir(base, user_ids_str, include_labels)
    else:
        raise FileNotFoundError(f"GeoLife path not found or unsupported format: {path}")


def read_geolife(
    path: str | Path,
    user_ids: list | None = None,
    include_labels: bool = True,
) -> dict[str, pd.DataFrame]:
    """Read GeoLife dataset into memory.

    Returns a dict mapping user_id -> DataFrame. For large datasets prefer
    iter_geolife() to avoid loading all users at once.
    """
    return {uid: df for uid, df in iter_geolife(path, user_ids, include_labels)}
