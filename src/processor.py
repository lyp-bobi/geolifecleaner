"""
Trajectory data processing: filtering, cleaning, segmentation, spatial transforms.
"""

import numpy as np
import pandas as pd

_EARTH_RADIUS_M = 6_371_000.0
_M_PER_DEG_LAT = 111_320.0          # metres per degree of latitude (approx)


# ── helpers ──────────────────────────────────────────────────────────────────

def _haversine_vs_prev(df: pd.DataFrame, lat_col: str, lon_col: str) -> np.ndarray:
    """Haversine distance (m) from each point to the previous point (first = 0)."""
    lat = np.radians(df[lat_col].values)
    lon = np.radians(df[lon_col].values)
    dlat = np.diff(lat, prepend=lat[0])
    dlon = np.diff(lon, prepend=lon[0])
    a = np.sin(dlat / 2) ** 2 + np.cos(lat) * np.cos(np.roll(lat, 1)) * np.sin(dlon / 2) ** 2
    a[0] = 0.0
    return 2 * _EARTH_RADIUS_M * np.arcsin(np.sqrt(np.clip(a, 0, 1)))


def _haversine_scalar(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    lat1, lon1, lat2, lon2 = map(np.radians, (lat1, lon1, lat2, lon2))
    dlat, dlon = lat2 - lat1, lon2 - lon1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    return float(2 * _EARTH_RADIUS_M * np.arcsin(np.sqrt(a)))


# ── basic filters ─────────────────────────────────────────────────────────────

def filter_bbox(df: pd.DataFrame, bbox: list,
                lat_col="latitude", lon_col="longitude") -> pd.DataFrame:
    min_lon, min_lat, max_lon, max_lat = bbox
    mask = (
        (df[lat_col] >= min_lat) & (df[lat_col] <= max_lat) &
        (df[lon_col] >= min_lon) & (df[lon_col] <= max_lon)
    )
    return df[mask].reset_index(drop=True)


def filter_date_range(df: pd.DataFrame, date_range: list,
                      ts_col="timestamp") -> pd.DataFrame:
    start, end = pd.Timestamp(date_range[0]), pd.Timestamp(date_range[1])
    mask = (df[ts_col] >= start) & (df[ts_col] <= end)
    return df[mask].reset_index(drop=True)


def drop_invalid_coords(df: pd.DataFrame,
                        lat_col="latitude", lon_col="longitude") -> pd.DataFrame:
    mask = (
        df[lat_col].notna() & df[lon_col].notna() &
        df[lat_col].between(-90, 90) &
        df[lon_col].between(-180, 180)
    )
    return df[mask].reset_index(drop=True)


def remove_duplicates(df: pd.DataFrame, id_col: str,
                      ts_col="timestamp", lat_col="latitude",
                      lon_col="longitude") -> pd.DataFrame:
    return df.drop_duplicates(
        subset=[id_col, ts_col, lat_col, lon_col]
    ).reset_index(drop=True)


# ── point-level outlier filters ───────────────────────────────────────────────

def filter_outlier_edge(
    df: pd.DataFrame,
    max_distance_m: float,
    id_col: str,
    lat_col="latitude",
    lon_col="longitude",
    ts_col="timestamp",
) -> pd.DataFrame:
    """Remove points whose straight-line distance to the previous point
    exceeds max_distance_m metres (catches GPS teleportation jumps)."""
    df = df.sort_values([id_col, ts_col]).reset_index(drop=True)
    keep = np.ones(len(df), dtype=bool)
    for _, g in df.groupby(id_col):
        dist = _haversine_vs_prev(g, lat_col, lon_col)
        keep[g.index] = dist <= max_distance_m
    return df[keep].reset_index(drop=True)


def filter_max_speed(
    df: pd.DataFrame,
    threshold_ms: float,
    id_col: str,
    lat_col="latitude",
    lon_col="longitude",
    ts_col="timestamp",
) -> pd.DataFrame:
    """Remove points where the instantaneous speed to the previous point
    exceeds threshold_ms metres per second."""
    df = df.sort_values([id_col, ts_col]).reset_index(drop=True)
    keep = np.ones(len(df), dtype=bool)
    for _, g in df.groupby(id_col):
        dist = _haversine_vs_prev(g, lat_col, lon_col)
        dt = g[ts_col].diff().dt.total_seconds().fillna(0).values
        with np.errstate(divide="ignore", invalid="ignore"):
            speed = np.where(dt > 0, dist / dt, 0.0)
        keep[g.index] = speed <= threshold_ms
    return df[keep].reset_index(drop=True)


# ── neighborhood deviation filter ────────────────────────────────────────────

def _neighborhood_mask(group: pd.DataFrame, window_points: int,
                       window_seconds: float, threshold_ratio: float,
                       lat_col: str, lon_col: str, ts_col: str) -> pd.Series:
    """Per-group implementation of the local neighborhood deviation filter.

    For each point i:
      1. Expand a window left/right up to window_points neighbours, stopping
         when the time gap between adjacent points exceeds window_seconds.
      2. If the window has fewer than 5 points (including i), keep the point.
      3. Compute the centroid of all window points *except* i.
      4. If  |dev_i| / (sum|dev_neighbours| + 0.01)  > threshold_ratio, drop.

    Coordinates are used in degrees (lon, lat); the ratio is dimensionless so
    scale does not matter for points in the same local neighbourhood.
    """
    orig_index = group.index
    g = group.reset_index(drop=True)
    n = len(g)
    if n < 2:
        return pd.Series(True, index=orig_index)

    xs = g[lon_col].values
    ys = g[lat_col].values
    ts = g[ts_col].values  # datetime64 (resolution depends on pandas version)

    keep = np.ones(n, dtype=bool)
    for i in range(n):
        lb, hb = i, i
        for j in range(1, window_points + 1):
            if lb - 1 >= 0 and float((ts[lb] - ts[lb - 1]) / np.timedelta64(1, "s")) <= window_seconds:
                lb -= 1
            if hb + 1 < n and float((ts[hb + 1] - ts[hb]) / np.timedelta64(1, "s")) <= window_seconds:
                hb += 1

        rlen = hb - lb + 1          # total points in window including i
        if rlen < 5:
            continue                 # too few neighbours → keep

        neighbours_x = np.concatenate([xs[lb:i], xs[i + 1:hb + 1]])
        neighbours_y = np.concatenate([ys[lb:i], ys[i + 1:hb + 1]])
        avg_x = neighbours_x.mean()
        avg_y = neighbours_y.mean()

        dev_i = abs(xs[i] - avg_x) + abs(ys[i] - avg_y)
        dev_sum = np.sum(np.abs(neighbours_x - avg_x) + np.abs(neighbours_y - avg_y))

        if dev_i / (dev_sum + 0.01) > threshold_ratio:
            keep[i] = False

    return pd.Series(keep, index=orig_index)


def filter_neighborhood(
    df: pd.DataFrame,
    window_points: int,
    window_seconds: float,
    threshold_ratio: float,
    id_col: str,
    lat_col="latitude",
    lon_col="longitude",
    ts_col="timestamp",
) -> pd.DataFrame:
    """Remove points that deviate anomalously from their local neighbourhood.

    Ported from the original filterOutlier.py `filter()` function.
    """
    df = df.sort_values([id_col, ts_col]).reset_index(drop=True)
    keep = np.ones(len(df), dtype=bool)
    for _, g in df.groupby(id_col):
        mask = _neighborhood_mask(
            g, window_points, window_seconds, threshold_ratio,
            lat_col, lon_col, ts_col,
        )
        keep[g.index] = mask.values
    return df[keep].reset_index(drop=True)


# ── segmentation helpers ──────────────────────────────────────────────────────

def _gap_cut_mask(group: pd.DataFrame, threshold_s: float,
                  ts_col: str) -> pd.Series:
    """True at the first point of a new segment created by a time gap."""
    dt = group[ts_col].diff().dt.total_seconds()
    return (dt > threshold_s).fillna(False)


def _stay_cut_mask(group: pd.DataFrame, time_s: float, dist_m: float,
                   lat_col: str, lon_col: str, ts_col: str) -> pd.Series:
    """True at positions where a new segment starts due to a stationary stay.

    A stay is detected when consecutive points all remain within dist_m metres
    of the first point in the cluster for at least time_s seconds.  The
    trajectory is split just before the stay begins and just after it ends.
    """
    orig_index = group.index
    g = group.reset_index(drop=True)
    n = len(g)
    if n < 2:
        return pd.Series(False, index=orig_index)

    lats = g[lat_col].values
    lons = g[lon_col].values
    times = g[ts_col].values  # datetime64 (resolution depends on pandas version)

    cut = np.zeros(n, dtype=bool)
    i = 0
    while i < n:
        j = i + 1
        while j < n and _haversine_scalar(lats[i], lons[i], lats[j], lons[j]) <= dist_m:
            j += 1
        # points [i, j) are all within dist_m of point i
        duration = float((times[j - 1] - times[i]) / np.timedelta64(1, "s")) if j > i else 0.0
        if j > i + 1 and duration >= time_s:
            # stay found at [i, j)
            if i > 0:
                cut[i] = True   # new segment starts at stay
            if j < n:
                cut[j] = True   # new segment starts after stay
            i = j
        else:
            i += 1

    return pd.Series(cut, index=orig_index)


# ── main entry point ──────────────────────────────────────────────────────────

# ── timestamp shifting ────────────────────────────────────────────────────────

def shift_timestamps(
    df: pd.DataFrame,
    range_start: str,
    range_end: str,
    id_col: str,
    ts_col: str = "timestamp",
    seed: int | None = None,
) -> pd.DataFrame:
    """Shift each trajectory segment's timestamps so that the midpoint
    (mean of first and last timestamp) falls at a uniformly random position
    within [range_start, range_end].  Relative spacing between points is
    preserved exactly.

    If seg_id is present, each (id, seg_id) segment is shifted independently.
    """
    t0 = pd.Timestamp(range_start)
    t1 = pd.Timestamp(range_end)
    if t1 <= t0:
        raise ValueError("time_shift.range_end must be after range_start")

    rng = np.random.default_rng(seed)
    # Work in whole seconds: GPS timestamps have at most second-level precision,
    # and integer-second Timedeltas are losslessly compatible with both
    # datetime64[ns] (pandas 2) and datetime64[us] (pandas 3).
    range_s = int((t1 - t0).total_seconds())

    group_cols = [id_col, "seg_id"] if "seg_id" in df.columns else [id_col]
    result = df.copy()
    ts_dtype = df[ts_col].dtype  # preserve original resolution

    for _, grp in df.groupby(group_cols):
        t_first = grp[ts_col].min()
        t_last = grp[ts_col].max()
        # midpoint in whole seconds
        mid_s = int(t_first.timestamp()) + (int(t_last.timestamp()) - int(t_first.timestamp())) // 2
        target_s = int(t0.timestamp()) + int(rng.integers(0, range_s + 1))
        offset = pd.Timedelta(seconds=int(target_s - mid_s))
        shifted = (grp[ts_col] + offset).astype(ts_dtype)
        result.loc[grp.index, ts_col] = shifted.values

    return result


# ── spatial transforms (rotation & displacement) ──────────────────────────────

def rotate_spatial(
    df: pd.DataFrame,
    distribution: str,
    std_deg: float,
    range_deg: float,
    per_segment: bool,
    seed,
    id_col: str,
    lat_col: str = "latitude",
    lon_col: str = "longitude",
) -> pd.DataFrame:
    """Rotate each trajectory (or segment) around its own bounding-box centre.

    The rotation angle is drawn from:
      • normal(0, std_deg)                      if distribution == 'normal'
      • uniform(-range_deg, +range_deg)          if distribution == 'uniform'

    Coordinates are projected to a local metric plane via an equirectangular
    approximation (accurate within ~0.1 % for city-scale trajectories), rotated,
    then projected back to WGS-84 degrees.
    """
    rng = np.random.default_rng(seed)
    group_cols = (
        [id_col, "seg_id"] if (per_segment and "seg_id" in df.columns) else [id_col]
    )
    result = df.copy()

    for _, grp in df.groupby(group_cols, sort=False):
        c_lat = (grp[lat_col].min() + grp[lat_col].max()) / 2.0
        c_lon = (grp[lon_col].min() + grp[lon_col].max()) / 2.0

        if distribution == "uniform":
            theta = np.radians(rng.uniform(-range_deg, range_deg))
        else:  # normal
            theta = np.radians(rng.normal(0.0, std_deg))

        cos_c = np.cos(np.radians(c_lat))          # longitude scaling factor
        m_per_deg_lon = _M_PER_DEG_LAT * cos_c

        # project to local metres (x = east, y = north)
        dx = (grp[lon_col].values - c_lon) * m_per_deg_lon
        dy = (grp[lat_col].values - c_lat) * _M_PER_DEG_LAT

        # 2-D rotation matrix
        cos_t, sin_t = np.cos(theta), np.sin(theta)
        dx_r = dx * cos_t - dy * sin_t
        dy_r = dx * sin_t + dy * cos_t

        # back to degrees
        result.loc[grp.index, lon_col] = c_lon + dx_r / m_per_deg_lon
        result.loc[grp.index, lat_col] = c_lat + dy_r / _M_PER_DEG_LAT

    return result


def displace_spatial(
    df: pd.DataFrame,
    distribution: str,
    std_deg: float,
    range_deg: float,
    per_segment: bool,
    seed,
    id_col: str,
    lat_col: str = "latitude",
    lon_col: str = "longitude",
) -> pd.DataFrame:
    """Shift each trajectory (or segment) by a random lat/lon offset.

    Offsets are drawn in degrees:
      • normal(0, std_deg)            if distribution == 'normal'
      • uniform(−range_deg, +range_deg) if distribution == 'uniform'

    The same std/range is applied to both latitude and longitude.
    """
    rng = np.random.default_rng(seed)
    group_cols = (
        [id_col, "seg_id"] if (per_segment and "seg_id" in df.columns) else [id_col]
    )
    result = df.copy()

    for _, grp in df.groupby(group_cols, sort=False):
        if distribution == "uniform":
            dlat = float(rng.uniform(-range_deg, range_deg))
            dlon = float(rng.uniform(-range_deg, range_deg))
        else:  # normal
            dlat = float(rng.normal(0.0, std_deg))
            dlon = float(rng.normal(0.0, std_deg))

        result.loc[grp.index, lat_col] = grp[lat_col].values + dlat
        result.loc[grp.index, lon_col] = grp[lon_col].values + dlon

    return result


def process(
    df: pd.DataFrame,
    id_col: str,
    cfg: dict,
    lat_col="latitude",
    lon_col="longitude",
    ts_col="timestamp",
) -> pd.DataFrame:
    """Apply all configured processing steps to a trajectory DataFrame.

    cfg is the full config dict (containing 'processing' and per-dataset
    'bbox' / 'date_range' passed in from the caller).
    """
    proc = cfg.get("processing", {})
    flt = cfg.get("filters", {})

    # ── basic cleaning ────────────────────────────────────────────────────────
    if proc.get("drop_invalid_coords", True):
        df = drop_invalid_coords(df, lat_col, lon_col)

    if proc.get("remove_duplicates", True):
        df = remove_duplicates(df, id_col, ts_col, lat_col, lon_col)

    if flt.get("bbox"):
        df = filter_bbox(df, flt["bbox"], lat_col, lon_col)

    if flt.get("date_range"):
        df = filter_date_range(df, flt["date_range"], ts_col)

    # ── point-level outlier removal ───────────────────────────────────────────
    edge_cfg = proc.get("outlier_edge", {})
    if edge_cfg.get("enabled"):
        df = filter_outlier_edge(df, edge_cfg["max_distance_m"],
                                 id_col, lat_col, lon_col, ts_col)

    speed_cfg = proc.get("max_speed", {})
    if speed_cfg.get("enabled"):
        df = filter_max_speed(df, speed_cfg["threshold_ms"],
                              id_col, lat_col, lon_col, ts_col)

    nbr_cfg = proc.get("neighborhood_filter", {})
    if nbr_cfg.get("enabled"):
        df = filter_neighborhood(
            df,
            window_points=nbr_cfg.get("window_points", 10),
            window_seconds=nbr_cfg.get("window_seconds", 600),
            threshold_ratio=nbr_cfg.get("threshold_ratio", 0.6),
            id_col=id_col, lat_col=lat_col, lon_col=lon_col, ts_col=ts_col,
        )

    # ── trajectory segmentation ───────────────────────────────────────────────
    df = df.sort_values([id_col, ts_col]).reset_index(drop=True)

    gap_cfg = proc.get("cut_gap", {})
    stay_cfg = proc.get("cut_stay", {})
    any_cut = gap_cfg.get("enabled") or stay_cfg.get("enabled")

    if any_cut:
        # Use a plain numpy array to avoid pandas 3.x groupby-apply alignment quirks.
        # df has been reset_index(drop=True) so g.index values are valid positions.
        cut = np.zeros(len(df), dtype=bool)

        if gap_cfg.get("enabled"):
            for _, g in df.groupby(id_col):
                mask = _gap_cut_mask(g, gap_cfg["threshold_s"], ts_col).values
                cut[g.index] |= mask

        if stay_cfg.get("enabled"):
            for _, g in df.groupby(id_col):
                mask = _stay_cut_mask(
                    g,
                    stay_cfg["time_threshold_s"],
                    stay_cfg["spatial_threshold_m"],
                    lat_col, lon_col, ts_col,
                ).values
                cut[g.index] |= mask

        df["_cut"] = cut
        df["seg_id"] = df.groupby(id_col)["_cut"].cumsum().astype(int)
        df.drop(columns=["_cut"], inplace=True)

    # ── timestamp shifting ────────────────────────────────────────────────────
    shift_cfg = proc.get("time_shift", {})
    if shift_cfg.get("enabled"):
        df = shift_timestamps(
            df,
            range_start=shift_cfg["range_start"],
            range_end=shift_cfg["range_end"],
            id_col=id_col,
            ts_col=ts_col,
            seed=shift_cfg.get("seed"),
        )

    # ── spatial rotation ──────────────────────────────────────────────────────
    rot_cfg = proc.get("spatial_rotation", {})
    if rot_cfg.get("enabled"):
        df = rotate_spatial(
            df,
            distribution=rot_cfg.get("distribution", "normal"),
            std_deg=rot_cfg.get("std_deg", 30.0),
            range_deg=rot_cfg.get("range_deg", 180.0),
            per_segment=rot_cfg.get("per_segment", True),
            seed=rot_cfg.get("seed"),
            id_col=id_col,
            lat_col=lat_col,
            lon_col=lon_col,
        )

    # ── spatial displacement ──────────────────────────────────────────────────
    disp_cfg = proc.get("spatial_displacement", {})
    if disp_cfg.get("enabled"):
        df = displace_spatial(
            df,
            distribution=disp_cfg.get("distribution", "normal"),
            std_deg=disp_cfg.get("std_deg", 0.5),
            range_deg=disp_cfg.get("range_deg", 1.0),
            per_segment=disp_cfg.get("per_segment", True),
            seed=disp_cfg.get("seed"),
            id_col=id_col,
            lat_col=lat_col,
            lon_col=lon_col,
        )

    return df
