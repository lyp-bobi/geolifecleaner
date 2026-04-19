# GeoLife & T-Drive Trajectory Cleaner

A tool to read, clean, and export the [GeoLife](https://www.microsoft.com/en-us/research/publication/geolife-gps-trajectory-dataset-user-guide/) and [T-Drive](https://www.microsoft.com/en-us/research/publication/t-drive-trajectory-data-sample/) GPS trajectory datasets into WKT, PostGIS SQL, or GeoJSON LineString files.

[中文文档](README_zh.md)

## Datasets

| Dataset | Source | Format | Coverage |
|---------|--------|--------|----------|
| GeoLife | 182 users, ~17,621 trajectories | `.plt` files | Beijing & worldwide, 2007–2012 |
| T-Drive  | 10,357 taxis, ~15 million points | `.txt` CSV files | Beijing, Feb 2008 |

## Project Structure

```
geolifeCleaner/
├── data/                   # Place dataset zips or extracted folders here
├── output/                 # Processed files are written here
├── src/
│   ├── geolife_reader.py   # Parses PLT format + transport mode labels
│   ├── tdrive_reader.py    # Parses T-Drive CSV format
│   ├── processor.py        # Filtering, cleaning, segmentation, spatial transforms
│   ├── writer.py           # WKT / PostGIS SQL / GeoJSON output
│   └── stats.py            # Summary statistics
├── config.yaml             # Default config (most steps disabled)
├── config_full.yaml        # Full config (all steps enabled)
├── main.py                 # Entry point
└── requirements.txt
```

## Quick Start

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Place data files

Download the datasets from the links above and place the zip files (or extracted folders) directly under `data/`.

### 3. Configure

- `config.yaml` — default config, most processing steps disabled.
- `config_full.yaml` — all steps enabled; adjust thresholds to fit your data.

Edit whichever you use to set dataset paths, filters, and output format.

### 4. Run

```bash
python main.py                   # uses config.yaml
python main.py config_full.yaml  # full pipeline
python main.py my_cfg.yaml       # custom config
```

## Processing Pipeline

Steps run in this order; each is toggled independently in `config.yaml`:

| Step | Config key | Description |
|------|-----------|-------------|
| Drop invalid coords | `drop_invalid_coords` | Remove NaN / out-of-range lat/lon |
| Remove duplicates | `remove_duplicates` | Drop exact (id, timestamp, lat, lon) duplicates |
| Bbox filter | `input.<dataset>.bbox` | Keep points within `[min_lon, min_lat, max_lon, max_lat]` |
| Date range filter | `input.<dataset>.date_range` | Keep points within `["YYYY-MM-DD", "YYYY-MM-DD"]` |
| Outlier edge | `outlier_edge` | Drop points further than `max_distance_m` from the previous point |
| Max speed | `max_speed` | Drop points with instantaneous speed > `threshold_ms` m/s |
| Neighborhood filter | `neighborhood_filter` | Drop points whose deviation from the local centroid exceeds `threshold_ratio` of total spread |
| Gap cut | `cut_gap` | Split trajectory at time gaps > `threshold_s` seconds |
| Stay cut | `cut_stay` | Split trajectory around stationary stays |
| Time shift | `time_shift` | Randomly shift each segment's timestamps into a target date range, preserving intervals |
| Spatial rotation | `spatial_rotation` | Rotate each trajectory/segment around its bounding-box centre by a random angle |
| Spatial displacement | `spatial_displacement` | Shift each trajectory/segment by a random (dlat, dlon) offset |
| Replication | `replicas` | Run the full pipeline N times per trajectory with independent random draws |

Segmentation steps (`cut_gap`, `cut_stay`) assign a `seg_id`; the writer emits one LineString per segment.

## Output Formats

Controlled by `output.format`:

| Format | Extension | Description |
|--------|-----------|-------------|
| `wkt` | `.wkt` | One `LINESTRING Z` or `LINESTRINGM` per line |
| `postgis` | `.sql` | `INSERT INTO <table>(traj) VALUES ('...'::geometry);` statements |
| `geojson` | `.geojson` | GeoJSON `FeatureCollection` of `LineString` features with timestamps in properties |

For `wkt` and `postgis`, time is stored in the Z or M coordinate (`output.time_dimension: z|m`) in units of Unix seconds or hours (`output.time_unit: seconds|hours`).

## Configuration

All options are documented inline in `config.yaml` and `config_full.yaml`.

## License

This tool is released under the MIT License. The datasets are subject to their own licenses — see the Microsoft Research pages linked above.
