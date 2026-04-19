"""
Output writer: WKT, PostGIS SQL, GeoJSON (LineString per trajectory segment).

Use StreamingWriter as a context manager to write one user at a time without
accumulating all results in memory.  write() is a thin convenience wrapper.
"""

import json
from pathlib import Path

import numpy as np
import pandas as pd


def _ensure_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)


# ── WKT / PostGIS builder ─────────────────────────────────────────────────────

_WKT_TYPE = {
    "z": "LINESTRING Z",
    "m": "LINESTRINGM",
}


def _build_linestring(group: pd.DataFrame, time_dim: str, time_unit: str,
                      lat_col: str, lon_col: str,
                      ts_col: str) -> str | None:
    """Return a WKT LINESTRING string for one trajectory segment, or None if
    the segment has fewer than 2 points.

    time_dim:  'z' → time in Z coordinate; 'm' → time in M (measure) value
    time_unit: 'seconds' → Unix epoch s; 'hours' → Unix epoch / 3600
    """
    group = group.sort_values(ts_col)
    if len(group) < 2:
        return None

    lons = group[lon_col].values
    lats = group[lat_col].values

    # Floor to seconds then compute Unix epoch as integer.
    # Using dt.floor('s') + astype("datetime64[s]") + view(np.int64) avoids
    # any pandas 2 (datetime64[ns]) vs pandas 3 (datetime64[us]) ambiguity.
    epochs_s = group[ts_col].dt.floor("s").astype("datetime64[s]").to_numpy().view(np.int64)
    if time_unit == "hours":
        time_vals = epochs_s / 3600.0
        fmt_t = lambda v: f"{v:.4f}"
    else:
        time_vals = epochs_s
        fmt_t = lambda v: str(int(v))

    if time_dim not in _WKT_TYPE:
        raise ValueError(f"Unknown time_dimension '{time_dim}'. Use: z, m")

    parts = [f"{lo} {la} {fmt_t(tv)}" for lo, la, tv in zip(lons, lats, time_vals)]
    return f"{_WKT_TYPE[time_dim]}({', '.join(parts)})"


def _iter_linestrings(data: dict, id_col: str, ts_col: str,
                      lat_col: str, lon_col: str,
                      time_dim: str, time_unit: str):
    """Yield (uid, seg_id, wkt) for every segment across all trajectories."""
    for uid, df in data.items():
        if "seg_id" in df.columns:
            groups = df.groupby("seg_id")
        else:
            groups = [(0, df)]
        for seg_id, group in groups:
            wkt = _build_linestring(group, time_dim, time_unit, lat_col, lon_col, ts_col)
            if wkt is not None:
                yield uid, seg_id, wkt


# ── GeoJSON feature builder ───────────────────────────────────────────────────

def _build_geojson_feature(uid, seg_id, group: pd.DataFrame,
                           id_col: str, ts_col: str,
                           lat_col: str, lon_col: str) -> dict | None:
    group = group.sort_values(ts_col)
    if len(group) < 2:
        return None

    coords = list(zip(group[lon_col].tolist(), group[lat_col].tolist()))
    timestamps = [str(t) for t in group[ts_col]]

    props = {
        id_col: str(uid),
        "seg_id": int(seg_id),
        "point_count": len(group),
        "start_time": timestamps[0],
        "end_time": timestamps[-1],
        "timestamps": timestamps,
    }

    if "transport_mode" in group.columns:
        valid = group["transport_mode"].dropna()
        props["transport_mode"] = valid.mode().iloc[0] if not valid.empty else None

    return {
        "type": "Feature",
        "geometry": {"type": "LineString", "coordinates": coords},
        "properties": props,
    }


def _iter_geojson_features(data: dict, id_col: str, ts_col: str,
                           lat_col: str, lon_col: str):
    """Yield GeoJSON feature dicts for every segment across all trajectories."""
    for uid, df in data.items():
        if "seg_id" in df.columns:
            groups = df.groupby("seg_id")
        else:
            groups = [(0, df)]
        for seg_id, group in groups:
            feat = _build_geojson_feature(uid, seg_id, group, id_col, ts_col, lat_col, lon_col)
            if feat is not None:
                yield feat


# ── format writers (single-batch) ────────────────────────────────────────────

def write_wkt(data: dict, filepath: Path, id_col: str, ts_col: str,
              lat_col: str, lon_col: str, time_dim: str, time_unit: str) -> int:
    _ensure_dir(filepath.parent)
    count = 0
    with open(filepath, "w", encoding="utf-8") as f:
        for _uid, _seg, wkt in _iter_linestrings(
                data, id_col, ts_col, lat_col, lon_col, time_dim, time_unit):
            f.write(wkt + "\n")
            count += 1
    return count


def write_postgis(data: dict, filepath: Path, id_col: str, ts_col: str,
                  lat_col: str, lon_col: str, time_dim: str, time_unit: str,
                  table="traj_table") -> int:
    _ensure_dir(filepath.parent)
    count = 0
    with open(filepath, "w", encoding="utf-8") as f:
        for _uid, _seg, wkt in _iter_linestrings(
                data, id_col, ts_col, lat_col, lon_col, time_dim, time_unit):
            f.write(f"INSERT INTO {table}(traj) VALUES ('{wkt}'::geometry);\n")
            count += 1
    return count


def write_geojson(data: dict, filepath: Path, id_col: str, ts_col: str,
                  lat_col: str, lon_col: str) -> int:
    _ensure_dir(filepath.parent)
    features = list(_iter_geojson_features(data, id_col, ts_col, lat_col, lon_col))
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(
            {"type": "FeatureCollection", "features": features},
            f, ensure_ascii=False, separators=(",", ":"),
        )
    return len(features)


# ── StreamingWriter ───────────────────────────────────────────────────────────

class StreamingWriter:
    """Write trajectory segments one user at a time.

    Use as a context manager.  Call write_user(uid, df) for each processed
    user; the output is flushed immediately so memory stays bounded.

    merge=True  → all users go into one file; the file is opened on __enter__
                  and finalised on __exit__.
    merge=False → each user gets its own file written and closed immediately.
    """

    def __init__(
        self,
        output_dir: Path,
        fmt: str,
        prefix: str,
        merge: bool,
        id_col: str,
        ts_col: str,
        lat_col: str,
        lon_col: str,
        out_cfg: dict,
    ):
        self._out = Path(output_dir)
        self._fmt = fmt.lower()
        self._prefix = prefix
        self._merge = merge
        self._id_col = id_col
        self._ts_col = ts_col
        self._lat_col = lat_col
        self._lon_col = lon_col
        self._time_dim = out_cfg.get("time_dimension", "m")
        self._time_unit = out_cfg.get("time_unit", "seconds")
        self._table = out_cfg.get("postgis_table", "traj_table")

        _ext = {"wkt": ".wkt", "postgis": ".sql", "geojson": ".geojson"}
        if self._fmt not in _ext:
            raise ValueError(f"Unsupported format '{self._fmt}'. Choose: wkt, postgis, geojson")
        self._ext = _ext[self._fmt]

        self._fh = None           # file handle for merge mode
        self._geojson_first = True
        self.total_segments = 0

    def __enter__(self):
        if self._merge:
            _ensure_dir(self._out)
            path = self._out / f"{self._prefix}{self._ext}"
            self._fh = open(path, "w", encoding="utf-8")
            if self._fmt == "geojson":
                self._fh.write('{"type":"FeatureCollection","features":[')
                self._geojson_first = True
        return self

    def __exit__(self, *_):
        if self._fh is not None:
            if self._fmt == "geojson":
                self._fh.write("]}")
            self._fh.close()
            self._fh = None

    def write_user(self, uid, df: pd.DataFrame) -> int:
        """Process and write one user's DataFrame. Returns segment count."""
        if self._merge:
            return self._append(uid, df)
        return self._write_file(uid, df)

    # ── merge-mode helpers ────────────────────────────────────────────────────

    def _append(self, uid, df: pd.DataFrame) -> int:
        count = 0
        if self._fmt in ("wkt", "postgis"):
            for _, _, wkt in _iter_linestrings(
                    {uid: df}, self._id_col, self._ts_col,
                    self._lat_col, self._lon_col,
                    self._time_dim, self._time_unit):
                if self._fmt == "wkt":
                    self._fh.write(wkt + "\n")
                else:
                    self._fh.write(
                        f"INSERT INTO {self._table}(traj) VALUES ('{wkt}'::geometry);\n"
                    )
                count += 1
        elif self._fmt == "geojson":
            for feat in _iter_geojson_features(
                    {uid: df}, self._id_col, self._ts_col,
                    self._lat_col, self._lon_col):
                if not self._geojson_first:
                    self._fh.write(",")
                json.dump(feat, self._fh, ensure_ascii=False, separators=(",", ":"))
                self._geojson_first = False
                count += 1
        self._fh.flush()
        self.total_segments += count
        return count

    # ── per-user file helpers ─────────────────────────────────────────────────

    def _write_file(self, uid, df: pd.DataFrame) -> int:
        path = self._out / f"{self._prefix}_{uid}{self._ext}"
        _ensure_dir(self._out)
        if self._fmt == "wkt":
            count = write_wkt({uid: df}, path, self._id_col, self._ts_col,
                              self._lat_col, self._lon_col, self._time_dim, self._time_unit)
        elif self._fmt == "postgis":
            count = write_postgis({uid: df}, path, self._id_col, self._ts_col,
                                  self._lat_col, self._lon_col, self._time_dim, self._time_unit,
                                  table=self._table)
        else:
            count = write_geojson({uid: df}, path, self._id_col, self._ts_col,
                                  self._lat_col, self._lon_col)
        self.total_segments += count
        return count


# ── convenience wrapper ───────────────────────────────────────────────────────

def write(
    data: dict,
    output_dir: str | Path,
    fmt: str,
    prefix: str,
    merge: bool,
    id_col: str,
    ts_col: str,
    lat_col: str,
    lon_col: str,
    out_cfg: dict,
):
    """Write trajectory data to output files.

    Thin wrapper around StreamingWriter that accepts a pre-built data dict.
    Prefer using StreamingWriter directly in processing loops to avoid
    accumulating all results in memory.
    """
    with StreamingWriter(
        output_dir=Path(output_dir),
        fmt=fmt,
        prefix=prefix,
        merge=merge,
        id_col=id_col,
        ts_col=ts_col,
        lat_col=lat_col,
        lon_col=lon_col,
        out_cfg=out_cfg,
    ) as sw:
        for uid, df in data.items():
            n = sw.write_user(uid, df)
            print(f"  Wrote {n} segments → {uid}")
    print(f"  Total: {sw.total_segments} segments")
