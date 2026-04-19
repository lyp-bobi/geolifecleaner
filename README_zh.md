# GeoLife & T-Drive 轨迹数据清洗工具

本工具用于读取、清洗并导出微软研究院公开的 [GeoLife](https://www.microsoft.com/en-us/research/publication/geolife-gps-trajectory-dataset-user-guide/) 和 [T-Drive](https://www.microsoft.com/en-us/research/publication/t-drive-trajectory-data-sample/) GPS 轨迹数据集，输出为 WKT、PostGIS SQL 或 GeoJSON LineString 格式。

[English Documentation](README.md)

## 数据集简介

| 数据集 | 来源 | 格式 | 覆盖范围 |
|--------|------|------|---------|
| GeoLife | 182 名用户，约 17,621 条轨迹 | `.plt` 文件 | 北京及全球，2007–2012 年 |
| T-Drive | 10,357 辆出租车，约 1500 万个 GPS 点 | `.txt` CSV 文件 | 北京，2008 年 2 月 |

## 项目结构

```
geolifeCleaner/
├── data/                   # 将数据集压缩包或解压后的文件夹放在此处
├── output/                 # 处理后的文件输出至此
├── src/
│   ├── geolife_reader.py   # 解析 PLT 格式及出行方式标签
│   ├── tdrive_reader.py    # 解析 T-Drive CSV 格式
│   ├── processor.py        # 过滤、清洗、分段、空间变换
│   ├── writer.py           # WKT / PostGIS SQL / GeoJSON 输出
│   └── stats.py            # 统计摘要
├── config.yaml             # 默认配置（大多数步骤关闭）
├── config_full.yaml        # 完整配置（所有步骤开启）
├── main.py                 # 程序入口
└── requirements.txt
```

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 放置数据文件

从上方链接下载数据集，将压缩包（或解压后的文件夹）直接放在 `data/` 目录下。

### 3. 配置参数

- `config.yaml` — 默认配置，大多数处理步骤关闭。
- `config_full.yaml` — 所有步骤开启，按需调整阈值。

根据需要编辑对应文件，设置数据集路径、过滤条件和输出格式。

### 4. 运行

```bash
python main.py                   # 使用 config.yaml
python main.py config_full.yaml  # 完整流程
python main.py my_cfg.yaml       # 自定义配置
```

## 处理流程

各步骤按以下顺序执行，均可在 `config.yaml` 中单独开关：

| 步骤 | 配置项 | 说明 |
|------|--------|------|
| 无效坐标过滤 | `drop_invalid_coords` | 删除 NaN 或超出合法范围的经纬度 |
| 去重 | `remove_duplicates` | 删除 (id, 时间, 经纬度) 完全相同的重复点 |
| 空间范围过滤 | `input.<dataset>.bbox` | 保留 `[最小经度, 最小纬度, 最大经度, 最大纬度]` 范围内的点 |
| 时间范围过滤 | `input.<dataset>.date_range` | 保留指定日期范围内的点 |
| 边缘异常点过滤 | `outlier_edge` | 删除与前一点距离超过 `max_distance_m` 的点 |
| 最大速度过滤 | `max_speed` | 删除瞬时速度超过 `threshold_ms` 米/秒的点 |
| 邻域异常点过滤 | `neighborhood_filter` | 删除偏离局部邻域质心超过总体扩散范围 `threshold_ratio` 的点 |
| 时间间隔分段 | `cut_gap` | 在时间间隔超过 `threshold_s` 秒处切分轨迹 |
| 停留点分段 | `cut_stay` | 在检测到的静止停留段处切分轨迹 |
| 时间偏移 | `time_shift` | 将每段轨迹的时间戳随机平移至目标日期范围，保留点间时间间隔 |
| 空间旋转 | `spatial_rotation` | 将每段轨迹绕其包围框中心随机旋转一个角度 |
| 空间平移 | `spatial_displacement` | 将每段轨迹随机平移一个 (dlat, dlon) 偏移量 |
| 数据复制 | `replicas` | 对每条轨迹重复运行完整流程 N 次，每次独立采样随机值 |

分段步骤（`cut_gap`、`cut_stay`）会添加 `seg_id` 列；输出时每个分段生成一条 LineString。

## 输出格式

由 `output.format` 控制：

| 格式 | 扩展名 | 说明 |
|------|--------|------|
| `wkt` | `.wkt` | 每行一条 `LINESTRING Z` 或 `LINESTRINGM` |
| `postgis` | `.sql` | `INSERT INTO <table>(traj) VALUES ('...'::geometry);` 语句 |
| `geojson` | `.geojson` | GeoJSON `FeatureCollection`，每个 `LineString` 要素包含时间戳属性 |

对于 `wkt` 和 `postgis`，时间可存储在 Z 或 M 坐标中（`output.time_dimension: z|m`），单位为 Unix 秒或小时（`output.time_unit: seconds|hours`）。

## 配置项说明

所有选项均在 `config.yaml` 和 `config_full.yaml` 中有内联注释说明。

## 许可协议

本工具以 MIT 协议开源。数据集本身遵循微软研究院的相关协议，请参见上方链接。
