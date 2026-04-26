[TOC]



# **mission1_v2**

## 一、项目概述

### 1.1 基础数据表结构

**trip_data** - 行程轨迹数据（16列）：
- `trip_id` (integer) - 行程ID
- `devid` (bigint) - 车辆设备ID
- `log_date` (date) - 数据日期
- `lon` (double precision[]) - 经度数组
- `lat` (double precision[]) - 纬度数组
- `tms` (double precision[]) - Unix时间戳数组
- `roads` (integer[]) - 道路ID数组
- `time` (bigint[]) - 时间数组
- `frac` (double precision[]) - 行程进度数组
- `route` (integer[]) - 路径ID数组
- `route_heading` (text[]) - 航向角数组
- `distance_km` (double precision) - 总里程
- `duration` (interval) - 总耗时
- `start_time` (timestamp) - 开始时间
- `end_time` (timestamp) - 结束时间
- `speed_array` (double precision[]) - 速度数组

**car** - 车辆聚合统计数据（29列）：
- `device_id` (varchar(50)) - 车辆设备ID
- `trip_ids` (integer[]) - 关联行程ID列表
- `trips_distance` (double precision[]) - 行程距离列表
- `total_distance` (double precision) - 总里程
- `trips_total` (integer) - 总行程数
- `trips_total_0_2` ~ `trips_total_22_24` (integer) - 各2小时间隔行程数（12列）
- `total_distance_0_2` ~ `total_distance_22_24` (double precision) - 各2小时间隔里程（12列）

### 1.2 四层架构总览

```
┌─────────────────────────────────────────────────────────────────┐
│                      ADS 层（应用数据层）                         │
│  面向业务场景，预计算封装，毫秒级响应                              │
│  ads_car_portrait_summary / ads_anomaly_trip / ads_forecast_*   │
├─────────────────────────────────────────────────────────────────┤
│                      TDM 层（标签数据层）                         │
│  业务标签 + 特征工程，算法模型输入                                 │
│  tdm_tag_car_operation / tdm_tag_trip_diagnosis / tdm_feat_*    │
├─────────────────────────────────────────────────────────────────┤
│                      DW 层（统一数仓层）                          │
│  面向主题建模，数组展开为结构化行，原子度量                        │
│  dw_fact_trip / dw_fact_road_segment / dw_dim_car               │
├─────────────────────────────────────────────────────────────────┤
│                      ODS 层（贴源数据层）                         │
│  保持原始结构，数据溯源，增量同步                                  │
│  ods_trip_raw / ods_car_stat                                    │
└─────────────────────────────────────────────────────────────────┘
```

---

## 二、ODS 层（贴源数据层）

### 2.1 设计原则

- 保持数据原貌，与源系统结构一致
- 保留数组字段，便于数据溯源
- 支持增量同步，通过 `load_time` 追踪数据变化
- 仅做最小必要的数据清洗

### 2.2 表结构

#### ods_trip_raw（行程原始数据表）

```sql
-- ODS层：行程轨迹原始数据表
DROP TABLE IF EXISTS ods_trip_raw;

CREATE TABLE ods_trip_raw (
    trip_id INTEGER NOT NULL,
    devid BIGINT NOT NULL,
    log_date DATE NOT NULL,

    -- 轨迹数组字段（保持原貌，支持溯源）
    lon DOUBLE PRECISION[] NOT NULL,
    lat DOUBLE PRECISION[] NOT NULL,
    tms DOUBLE PRECISION[],
    roads INTEGER[],
    time BIGINT[],
    frac DOUBLE PRECISION[],
    route INTEGER[],
    route_heading TEXT[],
    speed_array DOUBLE PRECISION[],

    -- 预计算字段（源表已有）
    distance_km DOUBLE PRECISION,
    duration INTERVAL,
    start_time TIMESTAMP WITHOUT TIME ZONE,
    end_time TIMESTAMP WITHOUT TIME ZONE,

    -- 溯源字段
    load_time TIMESTAMP DEFAULT NOW(),
    update_time TIMESTAMP DEFAULT NOW(),

    PRIMARY KEY (trip_id)
);

-- 索引
CREATE INDEX idx_ods_trip_raw_devid ON ods_trip_raw(devid);
CREATE INDEX idx_ods_trip_raw_log_date ON ods_trip_raw(log_date);
CREATE INDEX idx_ods_trip_raw_start_time ON ods_trip_raw(start_time);
```

#### ods_car_stat（车辆聚合统计表）

```sql
-- ODS层：车辆聚合统计表
DROP TABLE IF EXISTS ods_car_stat;

CREATE TABLE ods_car_stat (
    device_id VARCHAR(50) NOT NULL,

    -- 关联的行程ID列表
    trip_ids INTEGER[],

    -- 行程距离列表
    trips_distance DOUBLE PRECISION[],

    -- 总计
    total_distance DOUBLE PRECISION,
    trips_total INTEGER,

    -- 按2小时间隔的行程数
    trips_total_0_2 INTEGER,
    trips_total_2_4 INTEGER,
    trips_total_4_6 INTEGER,
    trips_total_6_8 INTEGER,
    trips_total_8_10 INTEGER,
    trips_total_10_12 INTEGER,
    trips_total_12_14 INTEGER,
    trips_total_14_16 INTEGER,
    trips_total_16_18 INTEGER,
    trips_total_18_20 INTEGER,
    trips_total_20_22 INTEGER,
    trips_total_22_24 INTEGER,

    -- 按2小时间隔的里程
    total_distance_0_2 DOUBLE PRECISION,
    total_distance_2_4 DOUBLE PRECISION,
    total_distance_4_6 DOUBLE PRECISION,
    total_distance_6_8 DOUBLE PRECISION,
    total_distance_8_10 DOUBLE PRECISION,
    total_distance_10_12 DOUBLE PRECISION,
    total_distance_12_14 DOUBLE PRECISION,
    total_distance_14_16 DOUBLE PRECISION,
    total_distance_16_18 DOUBLE PRECISION,
    total_distance_18_20 DOUBLE PRECISION,
    total_distance_20_22 DOUBLE PRECISION,
    total_distance_22_24 DOUBLE PRECISION,

    -- 溯源字段
    load_time TIMESTAMP DEFAULT NOW(),

    PRIMARY KEY (device_id)
);
```

### 2.3 数据导入

```sql
-- 从原表导入行程数据
INSERT INTO ods_trip_raw (
    trip_id, devid, log_date,
    lon, lat, tms, roads, time, frac, route, route_heading, speed_array,
    distance_km, duration, start_time, end_time
)
SELECT
    trip_id, devid, log_date,
    lon, lat, tms, roads, time, frac, route, route_heading, speed_array,
    distance_km, duration, start_time, end_time
FROM trip_data
WHERE trip_id IS NOT NULL
ON CONFLICT (trip_id) DO NOTHING;

-- 从原表导入车辆统计数据
INSERT INTO ods_car_stat
SELECT * FROM car
ON CONFLICT (device_id) DO NOTHING;
```

---

## 三、DW 层（统一数仓层）

### 3.1 设计原则

- 原子性保障：度量数据不可再拆分
- 一致性维度：相同含义的维度保持统一定义
- 历史全量保留：不删除历史数据，支持趋势分析
- 数组展开：将数组字段转换为结构化行数据，提升查询性能

### 3.2 表结构

#### dw_fact_trip（行程事实表）

**粒度**：一行一个行程

**核心职责**：将 `ods_trip_raw` 中的数组字段展开为结构化的行程度量

```sql
-- DW层：行程事实表
DROP TABLE IF EXISTS dw_fact_trip;

CREATE TABLE dw_fact_trip (
    trip_id INTEGER PRIMARY KEY,
    devid BIGINT NOT NULL,
    log_date DATE NOT NULL,

    -- 时间维度
    start_time TIMESTAMP WITHOUT TIME ZONE,
    end_time TIMESTAMP WITHOUT TIME ZONE,
    start_hour INTEGER,
    end_hour INTEGER,
    start_day_of_week INTEGER,
    is_weekend BOOLEAN,

    -- 起终点坐标（从数组首尾提取）
    start_lon DOUBLE PRECISION,
    start_lat DOUBLE PRECISION,
    end_lon DOUBLE PRECISION,
    end_lat DOUBLE PRECISION,

    -- 网格坐标（grid_size = 0.02度）
    start_grid_x INTEGER,
    start_grid_y INTEGER,
    end_grid_x INTEGER,
    end_grid_y INTEGER,

    -- 行程度量
    distance_km DOUBLE PRECISION,
    duration_seconds DOUBLE PRECISION,
    point_count INTEGER,
    road_count INTEGER,

    -- 速度度量
    avg_speed_kph DOUBLE PRECISION,
    max_speed_kph DOUBLE PRECISION,

    -- 数据质量
    is_complete BOOLEAN DEFAULT TRUE,

    insert_time TIMESTAMP DEFAULT NOW()
);

-- 索引
CREATE INDEX idx_dw_trip_devid ON dw_fact_trip(devid);
CREATE INDEX idx_dw_trip_log_date ON dw_fact_trip(log_date);
CREATE INDEX idx_dw_trip_start_hour ON dw_fact_trip(start_hour);
CREATE INDEX idx_dw_trip_start_grid ON dw_fact_trip(start_grid_x, start_grid_y);
CREATE INDEX idx_dw_trip_end_grid ON dw_fact_trip(end_grid_x, end_grid_y);
```

**ETL 填充 SQL**：

```sql
-- 从 ODS 层填充行程事实表
INSERT INTO dw_fact_trip (
    trip_id, devid, log_date,
    start_time, end_time, start_hour, end_hour,
    start_day_of_week, is_weekend,
    start_lon, start_lat, end_lon, end_lat,
    start_grid_x, start_grid_y, end_grid_x, end_grid_y,
    distance_km, duration_seconds,
    point_count, road_count,
    avg_speed_kph, max_speed_kph
)
SELECT
    trip_id,
    devid,
    log_date,
    start_time,
    end_time,
    EXTRACT(HOUR FROM start_time)::INTEGER AS start_hour,
    EXTRACT(HOUR FROM end_time)::INTEGER AS end_hour,
    EXTRACT(DOW FROM start_time)::INTEGER + 1 AS start_day_of_week,
    EXTRACT(DOW FROM start_time) IN (0, 6) AS is_weekend,

    -- 起点：数组第一个元素
    lon[1] AS start_lon,
    lat[1] AS start_lat,
    -- 终点：数组最后一个元素
    lon[array_length(lon, 1)] AS end_lon,
    lat[array_length(lat, 1)] AS end_lat,

    -- 网格计算（grid_size = 0.02）
    FLOOR(lon[1] / 0.02)::INTEGER AS start_grid_x,
    FLOOR(lat[1] / 0.02)::INTEGER AS start_grid_y,
    FLOOR(lon[array_length(lon, 1)] / 0.02)::INTEGER AS end_grid_x,
    FLOOR(lat[array_length(lat, 1)] / 0.02)::INTEGER AS end_grid_y,

    COALESCE(distance_km, 0),
    EXTRACT(EPOCH FROM duration) AS duration_seconds,
    array_length(lon, 1) AS point_count,
    COALESCE(array_length(roads, 1), 0) AS road_count,

    -- 平均速度 = 总里程(km) / 总时长(h)
    CASE
        WHEN EXTRACT(EPOCH FROM duration) > 0
        THEN distance_km / (EXTRACT(EPOCH FROM duration) / 3600.0)
        ELSE 0
    END AS avg_speed_kph,

    -- 最大速度（从speed_array中取最大值）
    (SELECT MAX(s) FROM unnest(speed_array) AS s WHERE s IS NOT NULL) AS max_speed_kph

FROM ods_trip_raw
WHERE array_length(lon, 1) >= 2
ON CONFLICT (trip_id) DO UPDATE SET
    start_lon = EXCLUDED.start_lon,
    start_lat = EXCLUDED.start_lat,
    end_lon = EXCLUDED.end_lon,
    end_lat = EXCLUDED.end_lat,
    start_grid_x = EXCLUDED.start_grid_x,
    start_grid_y = EXCLUDED.start_grid_y,
    end_grid_x = EXCLUDED.end_grid_x,
    end_grid_y = EXCLUDED.end_grid_y,
    distance_km = EXCLUDED.distance_km,
    duration_seconds = EXCLUDED.duration_seconds,
    point_count = EXCLUDED.point_count,
    avg_speed_kph = EXCLUDED.avg_speed_kph,
    max_speed_kph = EXCLUDED.max_speed_kph;
```

#### dw_fact_road_segment（路段事实表）

**粒度**：一行一个行程中的一个路段（相邻两点之间）

**核心职责**：将 `roads[]`、`speed_array[]`、`lon[]`、`lat[]` 数组展开为路段级别的结构化记录，是异常诊断和速度预测的基础

```sql
-- DW层：路段事实表（行程-道路交叉表）
DROP TABLE IF EXISTS dw_fact_road_segment;

CREATE TABLE dw_fact_road_segment (
    segment_id BIGSERIAL PRIMARY KEY,
    trip_id INTEGER NOT NULL,
    road_id INTEGER NOT NULL,
    log_date DATE NOT NULL,

    -- 路段位置信息
    segment_index INTEGER NOT NULL,
    start_point_index INTEGER,
    end_point_index INTEGER,

    -- 该路段的起终点坐标
    start_lon DOUBLE PRECISION,
    start_lat DOUBLE PRECISION,
    end_lon DOUBLE PRECISION,
    end_lat DOUBLE PRECISION,

    -- 路段度量
    speed_kph DOUBLE PRECISION,
    heading_deg DOUBLE PRECISION,

    -- 时间信息
    tms DOUBLE PRECISION,
    start_hour INTEGER,

    -- 拥堵判断（基于 congestion_speed_kph = 20.0 阈值）
    is_congested BOOLEAN DEFAULT FALSE,
    congestion_intensity DOUBLE PRECISION,

    insert_time TIMESTAMP DEFAULT NOW()
);

-- 索引
CREATE INDEX idx_dws_road_id ON dw_fact_road_segment(road_id);
CREATE INDEX idx_dws_trip_id ON dw_fact_road_segment(trip_id);
CREATE INDEX idx_dws_log_date ON dw_fact_road_segment(log_date);
CREATE INDEX idx_dws_congested ON dw_fact_road_segment(road_id, start_hour) WHERE is_congested = TRUE;
```

**ETL 填充 SQL**：

```sql
-- 从 ODS 层填充路段事实表（展开数组）
INSERT INTO dw_fact_road_segment (
    trip_id, road_id, log_date,
    segment_index, start_point_index, end_point_index,
    start_lon, start_lat, end_lon, end_lat,
    speed_kph, tms, start_hour,
    is_congested, congestion_intensity
)
SELECT
    t.trip_id,
    t.roads[idx]::INTEGER AS road_id,
    t.log_date,
    idx AS segment_index,
    idx AS start_point_index,
    idx + 1 AS end_point_index,

    -- 当前点和下一个点的坐标
    t.lon[idx] AS start_lon,
    t.lat[idx] AS start_lat,
    t.lon[idx + 1] AS end_lon,
    t.lat[idx + 1] AS end_lat,

    -- 速度值
    sp.val AS speed_kph,
    t.tms[idx] AS tms,
    EXTRACT(HOUR FROM TO_TIMESTAMP(COALESCE(t.tms[idx], 0)))::INTEGER AS start_hour,

    -- 拥堵判断（阈值=20 km/h）
    CASE WHEN sp.val < 20.0 THEN TRUE ELSE FALSE END AS is_congested,
    -- 拥堵强度 = (阈值 - 速度) / 阈值，clamp到[0,1]
    GREATEST(0.0, LEAST(1.0, (20.0 - sp.val) / 20.0)) AS congestion_intensity

FROM ods_trip_raw t
CROSS JOIN LATERAL generate_series(1, array_length(t.roads, 1) - 1) AS idx
CROSS JOIN LATERAL (
    SELECT
        CASE
            WHEN t.speed_array IS NOT NULL
                 AND array_length(t.speed_array, 1) >= idx
            THEN t.speed_array[idx]
            ELSE NULL
        END AS val
) AS sp
WHERE t.roads[idx] IS NOT NULL
  AND t.lon[idx] IS NOT NULL AND t.lat[idx] IS NOT NULL
  AND t.lon[idx + 1] IS NOT NULL AND t.lat[idx + 1] IS NOT NULL
ON CONFLICT DO NOTHING;
```

#### dw_dim_car（车辆维度表）

**粒度**：一行一辆车

**核心职责**：从 `ods_car_stat` 和 `dw_fact_trip` 生成车辆维度的汇总信息，包括运营指标和2小时间隔分布

```sql
-- DW层：车辆维度表
DROP TABLE IF EXISTS dw_dim_car;

CREATE TABLE dw_dim_car (
    device_id VARCHAR(50) PRIMARY KEY,

    -- 基本运营指标
    total_trips INTEGER,
    total_distance_km DOUBLE PRECISION,
    avg_trip_distance_km DOUBLE PRECISION,
    avg_trip_duration_minutes DOUBLE PRECISION,

    -- 活跃度指标
    active_days INTEGER,
    first_trip_date DATE,
    last_trip_date DATE,

    -- 按2小时间隔的行程分布（用于活跃时段判断）
    trip_count_0_2 INTEGER,
    trip_count_2_4 INTEGER,
    trip_count_4_6 INTEGER,
    trip_count_6_8 INTEGER,
    trip_count_8_10 INTEGER,
    trip_count_10_12 INTEGER,
    trip_count_12_14 INTEGER,
    trip_count_14_16 INTEGER,
    trip_count_16_18 INTEGER,
    trip_count_18_20 INTEGER,
    trip_count_20_22 INTEGER,
    trip_count_22_24 INTEGER,

    -- 按2小时间隔的里程分布
    distance_0_2 DOUBLE PRECISION,
    distance_2_4 DOUBLE PRECISION,
    distance_4_6 DOUBLE PRECISION,
    distance_6_8 DOUBLE PRECISION,
    distance_8_10 DOUBLE PRECISION,
    distance_10_12 DOUBLE PRECISION,
    distance_12_14 DOUBLE PRECISION,
    distance_14_16 DOUBLE PRECISION,
    distance_16_18 DOUBLE PRECISION,
    distance_18_20 DOUBLE PRECISION,
    distance_20_22 DOUBLE PRECISION,
    distance_22_24 DOUBLE PRECISION,

    update_time TIMESTAMP DEFAULT NOW()
);
```

**ETL 填充 SQL**：

```sql
-- 从 ODS 层填充车辆维度表
INSERT INTO dw_dim_car (
    device_id, total_trips, total_distance_km,
    avg_trip_distance_km, avg_trip_duration_minutes,
    active_days, first_trip_date, last_trip_date,
    trip_count_0_2, trip_count_2_4, trip_count_4_6, trip_count_6_8,
    trip_count_8_10, trip_count_10_12, trip_count_12_14, trip_count_14_16,
    trip_count_16_18, trip_count_18_20, trip_count_20_22, trip_count_22_24,
    distance_0_2, distance_2_4, distance_4_6, distance_6_8,
    distance_8_10, distance_10_12, distance_12_14, distance_14_16,
    distance_16_18, distance_18_20, distance_20_22, distance_22_24
)
SELECT
    c.device_id,
    c.trips_total,
    c.total_distance,

    -- 平均行程距离
    CASE WHEN c.trips_total > 0 THEN c.total_distance / c.trips_total ELSE 0 END,
    -- 平均行程时长（从trip_data计算）
    (
        SELECT AVG(EXTRACT(EPOCH FROM t.duration) / 60.0)
        FROM ods_trip_raw t
        WHERE t.trip_id = ANY(c.trip_ids)
    ),

    -- 活跃天数
    (SELECT COUNT(DISTINCT t.log_date) FROM ods_trip_raw t WHERE t.trip_id = ANY(c.trip_ids)),
    (SELECT MIN(t.log_date) FROM ods_trip_raw t WHERE t.trip_id = ANY(c.trip_ids)),
    (SELECT MAX(t.log_date) FROM ods_trip_raw t WHERE t.trip_id = ANY(c.trip_ids)),

    -- 2小时间隔行程数
    c.trips_total_0_2, c.trips_total_2_4, c.trips_total_4_6,
    c.trips_total_6_8, c.trips_total_8_10, c.trips_total_10_12,
    c.trips_total_12_14, c.trips_total_14_16, c.trips_total_16_18,
    c.trips_total_18_20, c.trips_total_20_22, c.trips_total_22_24,

    -- 2小时间隔里程
    c.total_distance_0_2, c.total_distance_2_4, c.total_distance_4_6,
    c.total_distance_6_8, c.total_distance_8_10, c.total_distance_10_12,
    c.total_distance_12_14, c.total_distance_14_16, c.total_distance_16_18,
    c.total_distance_18_20, c.total_distance_20_22, c.total_distance_22_24

FROM ods_car_stat c
ON CONFLICT (device_id) DO UPDATE SET
    total_trips = EXCLUDED.total_trips,
    total_distance_km = EXCLUDED.total_distance_km,
    avg_trip_distance_km = EXCLUDED.avg_trip_distance_km,
    avg_trip_duration_minutes = EXCLUDED.avg_trip_duration_minutes,
    active_days = EXCLUDED.active_days,
    first_trip_date = EXCLUDED.first_trip_date,
    last_trip_date = EXCLUDED.last_trip_date,
    trip_count_0_2 = EXCLUDED.trip_count_0_2,
    trip_count_2_4 = EXCLUDED.trip_count_2_4,
    trip_count_4_6 = EXCLUDED.trip_count_4_6,
    trip_count_6_8 = EXCLUDED.trip_count_6_8,
    trip_count_8_10 = EXCLUDED.trip_count_8_10,
    trip_count_10_12 = EXCLUDED.trip_count_10_12,
    trip_count_12_14 = EXCLUDED.trip_count_12_14,
    trip_count_14_16 = EXCLUDED.trip_count_14_16,
    trip_count_16_18 = EXCLUDED.trip_count_16_18,
    trip_count_18_20 = EXCLUDED.trip_count_18_20,
    trip_count_20_22 = EXCLUDED.trip_count_20_22,
    trip_count_22_24 = EXCLUDED.trip_count_22_24,
    distance_0_2 = EXCLUDED.distance_0_2,
    distance_2_4 = EXCLUDED.distance_2_4,
    distance_4_6 = EXCLUDED.distance_4_6,
    distance_6_8 = EXCLUDED.distance_6_8,
    distance_8_10 = EXCLUDED.distance_8_10,
    distance_10_12 = EXCLUDED.distance_10_12,
    distance_12_14 = EXCLUDED.distance_12_14,
    distance_14_16 = EXCLUDED.distance_14_16,
    distance_16_18 = EXCLUDED.distance_16_18,
    distance_18_20 = EXCLUDED.distance_18_20,
    distance_20_22 = EXCLUDED.distance_20_22,
    distance_22_24 = EXCLUDED.distance_22_24,
    update_time = NOW();
```

---

## 四、TDM 层（标签数据层）

### 4.1 设计原则

- 业务洞察沉淀：将专家经验转化为可复用的数据标签
- 精准画像支撑：支持快速识别特定类型车辆、路段
- 算法特征输入：预计算特征值，加速模型训练和推理
- 标签阈值与 Python 代码中的常量完全对齐

### 4.2 表结构

#### tdm_tag_car_operation（车辆运营标签表）

**粒度**：一行一辆车×一个标签日期

**核心职责**：对应 `car_portrait.py` 中 `analyze_vehicle_operations` 和 `classify_operation_mode` 的输出

**标签逻辑**：
- `dominant_shift`：夜间/早高峰/日间/晚高峰/混合（占比≥35%为有效班次）
- `operation_mode`：night_shift / commuter_peak / long_haul / local_shuttle / steady_all_day
- 五种模式的判断规则与 `classify_operation_mode` 函数完全一致

```sql
-- TDM层：车辆运营标签表
DROP TABLE IF EXISTS tdm_tag_car_operation;

CREATE TABLE tdm_tag_car_operation (
    device_id VARCHAR(50) NOT NULL,
    tag_date DATE NOT NULL DEFAULT CURRENT_DATE,

    -- 运营规模
    total_trips INTEGER,
    total_distance_km DOUBLE PRECISION,
    avg_trip_distance_km DOUBLE PRECISION,
    avg_trip_duration_minutes DOUBLE PRECISION,

    -- 活跃度
    active_days INTEGER,
    avg_daily_work_hours DOUBLE PRECISION,

    -- 时段特征（来自 SHIFT_BUCKETS 定义）
    night_trip_ratio DOUBLE PRECISION,
    morning_peak_ratio DOUBLE PRECISION,
    daytime_ratio DOUBLE PRECISION,
    evening_peak_ratio DOUBLE PRECISION,
    dominant_shift VARCHAR(20),

    -- 运营模式（classify_operation_mode 的五种模式）
    operation_mode VARCHAR(30),

    -- 空间特征
    hotspot_concentration DOUBLE PRECISION,

    -- 同行分组
    peer_group_id INTEGER,

    -- 风险标签
    risk_level VARCHAR(10),

    compute_time TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (device_id, tag_date)
);

CREATE INDEX idx_tdm_car_op_device ON tdm_tag_car_operation(device_id);
CREATE INDEX idx_tdm_car_op_mode ON tdm_tag_car_operation(operation_mode);
```

**ETL 填充 SQL**：

```sql
-- 从 DW 层填充车辆运营标签表
INSERT INTO tdm_tag_car_operation (
    device_id, tag_date,
    total_trips, total_distance_km,
    avg_trip_distance_km, avg_trip_duration_minutes,
    active_days, avg_daily_work_hours,
    night_trip_ratio, morning_peak_ratio, daytime_ratio, evening_peak_ratio,
    dominant_shift, operation_mode, hotspot_concentration
)
SELECT
    c.device_id,
    CURRENT_DATE,

    c.total_trips,
    c.total_distance_km,
    c.avg_trip_distance_km,
    c.avg_trip_duration_minutes,

    c.active_days,
    -- avg_daily_work_hours
    CASE WHEN c.active_days > 0
         THEN (c.total_trips * c.avg_trip_duration_minutes / 60.0) / c.active_days
         ELSE 0
    END,

    -- 夜间占比：22-24 + 0-6 时段
    CASE WHEN c.total_trips > 0
         THEN (c.trip_count_22_24 + c.trip_count_0_2 + c.trip_count_2_4 + c.trip_count_4_6)::DOUBLE PRECISION / c.total_trips
         ELSE 0
    END,

    -- 早高峰占比：6-10 时段
    CASE WHEN c.total_trips > 0
         THEN (c.trip_count_6_8 + c.trip_count_8_10)::DOUBLE PRECISION / c.total_trips
         ELSE 0
    END,

    -- 日间占比：10-16 时段
    CASE WHEN c.total_trips > 0
         THEN (c.trip_count_10_12 + c.trip_count_12_14 + c.trip_count_14_16)::DOUBLE PRECISION / c.total_trips
         ELSE 0
    END,

    -- 晚高峰占比：16-22 时段
    CASE WHEN c.total_trips > 0
         THEN (c.trip_count_16_18 + c.trip_count_18_20 + c.trip_count_20_22)::DOUBLE PRECISION / c.total_trips
         ELSE 0
    END,

    -- dominant_shift 判断逻辑（对应 determine_dominant_shift）
    CASE
        WHEN c.total_trips <= 0 THEN 'mixed'
        WHEN GREATEST(
            (c.trip_count_22_24 + c.trip_count_0_2 + c.trip_count_2_4 + c.trip_count_4_6)::DOUBLE PRECISION,
            (c.trip_count_6_8 + c.trip_count_8_10)::DOUBLE PRECISION,
            (c.trip_count_10_12 + c.trip_count_12_14 + c.trip_count_14_16)::DOUBLE PRECISION,
            (c.trip_count_16_18 + c.trip_count_18_20 + c.trip_count_20_22)::DOUBLE PRECISION
        ) / c.total_trips < 0.35 THEN 'mixed'
        ELSE
            CASE
                WHEN (c.trip_count_22_24 + c.trip_count_0_2 + c.trip_count_2_4 + c.trip_count_4_6) >=
                     GREATEST((c.trip_count_6_8 + c.trip_count_8_10),
                              (c.trip_count_10_12 + c.trip_count_12_14 + c.trip_count_14_16),
                              (c.trip_count_16_18 + c.trip_count_18_20 + c.trip_count_20_22))
                THEN 'night'
                WHEN (c.trip_count_6_8 + c.trip_count_8_10) >=
                     GREATEST((c.trip_count_10_12 + c.trip_count_12_14 + c.trip_count_14_16),
                              (c.trip_count_16_18 + c.trip_count_18_20 + c.trip_count_20_22))
                THEN 'morning_peak'
                WHEN (c.trip_count_10_12 + c.trip_count_12_14 + c.trip_count_14_16) >=
                     (c.trip_count_16_18 + c.trip_count_18_20 + c.trip_count_20_22)
                THEN 'daytime'
                ELSE 'evening_peak'
            END
    END,

    -- operation_mode 判断逻辑（对应 classify_operation_mode）
    CASE
        WHEN c.total_trips <= 0 THEN 'steady_all_day'
        WHEN (c.trip_count_22_24 + c.trip_count_0_2 + c.trip_count_2_4 + c.trip_count_4_6)::DOUBLE PRECISION / c.total_trips >= 0.45
             THEN 'night_shift'
        WHEN (c.trip_count_6_8 + c.trip_count_8_10 + c.trip_count_16_18 + c.trip_count_18_20 + c.trip_count_20_22)::DOUBLE PRECISION / c.total_trips >= 0.55
             AND (c.total_trips * c.avg_trip_duration_minutes / 60.0) / NULLIF(c.active_days, 0) <= 10.5
             THEN 'commuter_peak'
        WHEN c.avg_trip_distance_km >= 13.0
             AND (c.total_trips * c.avg_trip_duration_minutes / 60.0) / NULLIF(c.active_days, 0) >= 6.0
             THEN 'long_haul'
        WHEN c.avg_trip_distance_km <= 5.0
             THEN 'local_shuttle'
        ELSE 'steady_all_day'
    END,

    -- hotspot_concentration 简化计算
    0.5

FROM dw_dim_car c
ON CONFLICT (device_id, tag_date) DO UPDATE SET
    total_trips = EXCLUDED.total_trips,
    total_distance_km = EXCLUDED.total_distance_km,
    avg_trip_distance_km = EXCLUDED.avg_trip_distance_km,
    avg_trip_duration_minutes = EXCLUDED.avg_trip_duration_minutes,
    active_days = EXCLUDED.active_days,
    avg_daily_work_hours = EXCLUDED.avg_daily_work_hours,
    night_trip_ratio = EXCLUDED.night_trip_ratio,
    morning_peak_ratio = EXCLUDED.morning_peak_ratio,
    daytime_ratio = EXCLUDED.daytime_ratio,
    evening_peak_ratio = EXCLUDED.evening_peak_ratio,
    dominant_shift = EXCLUDED.dominant_shift,
    operation_mode = EXCLUDED.operation_mode,
    hotspot_concentration = EXCLUDED.hotspot_concentration,
    compute_time = NOW();
```

#### tdm_tag_trip_diagnosis（行程异常诊断标签表）

**粒度**：一行一个行程

**核心职责**：对应 `diagnosis.py` 中 `analyze_trip_diagnosis` 的输出

**标签逻辑**：
- `risk_level`：high（high事件≥1或medium事件≥2）/ medium（medium事件=1或总事件≥2）/ low
- `anomaly_score`：100 - (high事件数×35 + medium事件数×20 + low事件数×10)
- 五种异常类型：detour（绕路）、stop（停留）、speed_jump（速度突变）、drift（漂移）、jump_point（跳点）

```sql
-- TDM层：行程异常诊断标签表
DROP TABLE IF EXISTS tdm_tag_trip_diagnosis;

CREATE TABLE tdm_tag_trip_diagnosis (
    trip_id INTEGER PRIMARY KEY,
    diagnosis_date DATE NOT NULL DEFAULT CURRENT_DATE,

    -- 异常汇总
    risk_level VARCHAR(10) NOT NULL,
    anomaly_score INTEGER,
    total_events INTEGER,

    -- 五种异常类型计数
    detour_count INTEGER DEFAULT 0,
    stop_count INTEGER DEFAULT 0,
    speed_jump_count INTEGER DEFAULT 0,
    drift_count INTEGER DEFAULT 0,
    jump_point_count INTEGER DEFAULT 0,

    -- 异常指标
    direct_distance_km DOUBLE PRECISION,
    actual_distance_km DOUBLE PRECISION,
    directness_ratio DOUBLE PRECISION,
    max_speed_kph DOUBLE PRECISION,
    stop_seconds_total DOUBLE PRECISION,
    repeated_road_ratio DOUBLE PRECISION,
    backtrack_count INTEGER,

    compute_time TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_tdm_diag_risk ON tdm_tag_trip_diagnosis(risk_level);
CREATE INDEX idx_tdm_diag_date ON tdm_tag_trip_diagnosis(diagnosis_date);
```

**ETL 填充 SQL**（基于 `dw_fact_road_segment`，使用 Python `diagnosis.py` 的算法逻辑）：

```sql
-- TDM层：行程异常诊断标签表 - ETL填充SQL
-- 基于 dw_fact_road_segment 表计算行程级别的异常诊断

WITH
-- 1. 路段带时间戳信息
segment_with_time AS (
    SELECT
        s.trip_id,
        s.segment_index,
        s.road_id,
        s.start_lon, s.start_lat,
        s.end_lon, s.end_lat,
        s.speed_kph, s.tms, s.heading_deg,
        s.tms AS segment_start_time,
        LEAD(s.tms) OVER (PARTITION BY s.trip_id ORDER BY s.segment_index) AS segment_end_time,
        t.distance_km AS total_distance_km,
        t.start_time AS trip_start_time,
        t.end_time AS trip_end_time
    FROM dw_fact_road_segment s
    JOIN dw_fact_trip t ON s.trip_id = t.trip_id
    WHERE s.tms IS NOT NULL
),

-- 2. 基础统计
trip_segment_stats AS (
    SELECT
        trip_id,
        COUNT(*) AS segment_count,
        MAX(speed_kph) AS max_speed_kph,
        (ARRAY_AGG(start_lon ORDER BY segment_index))[1] AS first_lon,
        (ARRAY_AGG(start_lat ORDER BY segment_index))[1] AS first_lat,
        (ARRAY_AGG(end_lon ORDER BY segment_index DESC))[1] AS last_lon,
        (ARRAY_AGG(end_lat ORDER BY segment_index DESC))[1] AS last_lat,
        MAX(total_distance_km) AS actual_distance_km
    FROM segment_with_time
    GROUP BY trip_id
),

-- 3. 速度突变检测：相邻路段速度差 >= 35 km/h
speed_jump_detection AS (
    SELECT
        s1.trip_id,
        COUNT(*) AS speed_jump_count
    FROM segment_with_time s1
    JOIN segment_with_time s2
        ON s1.trip_id = s2.trip_id AND s2.segment_index = s1.segment_index + 1
    WHERE s1.speed_kph IS NOT NULL AND s2.speed_kph IS NOT NULL
      AND ABS(s2.speed_kph - s1.speed_kph) >= 35.0
    GROUP BY s1.trip_id
),

-- 4. 停留检测：连续3分钟以上速度 < 5 km/h
stop_detection AS (
    SELECT
        trip_id,
        COUNT(*) AS stop_count,
        SUM(stop_duration) AS stop_seconds_total
    FROM (
        SELECT
            trip_id, speed_kph,
            (segment_end_time - segment_start_time) AS stop_duration
        FROM segment_with_time
        WHERE speed_kph IS NOT NULL AND speed_kph <= 5.0
          AND segment_end_time IS NOT NULL
          AND (segment_end_time - segment_start_time) >= 180.0
    ) stops
    GROUP BY trip_id
),

-- 5. 道路重复率
road_repeat AS (
    SELECT
        trip_id,
        CASE WHEN COUNT(*) > 0
             THEN SUM(CASE WHEN rc.cnt > 1 THEN rc.cnt ELSE 0 END)::DOUBLE PRECISION / COUNT(*)
             ELSE 0
        END AS repeated_road_ratio
    FROM (
        SELECT trip_id, road_id, COUNT(*) AS cnt
        FROM segment_with_time
        WHERE road_id IS NOT NULL
        GROUP BY trip_id, road_id
    ) rc
    GROUP BY trip_id
),

-- 6. 航向反转检测：相邻路段航向差 >= 150度
backtrack_detection AS (
    SELECT
        s1.trip_id,
        COUNT(*) AS backtrack_count
    FROM segment_with_time s1
    JOIN segment_with_time s2
        ON s1.trip_id = s2.trip_id AND s2.segment_index = s1.segment_index + 1
    WHERE s1.heading_deg IS NOT NULL AND s2.heading_deg IS NOT NULL
      AND ABS(s2.heading_deg - s1.heading_deg) BETWEEN 150.0 AND 210.0
    GROUP BY s1.trip_id
),

-- 7. 起终点直线距离（Haversine公式，无PostGIS）
direct_distance AS (
    SELECT
        trip_id,
        6371.0 * ACOS(
            LEAST(1.0, GREATEST(-1.0,
                SIN(RADIANS(first_lat)) * SIN(RADIANS(last_lat))
                + COS(RADIANS(first_lat)) * COS(RADIANS(last_lat))
                * COS(RADIANS(last_lon - first_lon))
            ))
        ) AS direct_distance_km
    FROM trip_segment_stats
    WHERE first_lon IS NOT NULL AND first_lat IS NOT NULL
      AND last_lon IS NOT NULL AND last_lat IS NOT NULL
),

-- 8. 绕路检测
detour_detection AS (
    SELECT
        tss.trip_id,
        CASE WHEN tss.actual_distance_km >= 2.0
              AND dd.direct_distance_km >= 0.8
              AND tss.actual_distance_km / NULLIF(dd.direct_distance_km, 0) >= 2.2
              AND (COALESCE(rr.repeated_road_ratio, 0) >= 0.25 OR COALESCE(bd.backtrack_count, 0) >= 1)
             THEN 1 ELSE 0
        END AS detour_count
    FROM trip_segment_stats tss
    LEFT JOIN direct_distance dd ON tss.trip_id = dd.trip_id
    LEFT JOIN road_repeat rr ON tss.trip_id = rr.trip_id
    LEFT JOIN backtrack_detection bd ON tss.trip_id = bd.trip_id
),

-- 9. 跳点检测：相邻点路径和/直线距离 >= 3
jump_point_detection AS (
    SELECT
        s1.trip_id,
        COUNT(*) AS jump_point_count
    FROM segment_with_time s1
    JOIN segment_with_time s2
        ON s1.trip_id = s2.trip_id AND s2.segment_index = s1.segment_index + 1
    WHERE s1.end_lon IS NOT NULL AND s1.end_lat IS NOT NULL
      AND s2.end_lon IS NOT NULL AND s2.end_lat IS NOT NULL
      AND (
        6371.0 * ACOS(LEAST(1.0, GREATEST(-1.0,
            SIN(RADIANS(s1.start_lat)) * SIN(RADIANS(s1.end_lat))
            + COS(RADIANS(s1.start_lat)) * COS(RADIANS(s1.end_lat))
            * COS(RADIANS(s1.end_lon - s1.start_lon))
        )))
        + 6371.0 * ACOS(LEAST(1.0, GREATEST(-1.0,
            SIN(RADIANS(s2.start_lat)) * SIN(RADIANS(s2.end_lat))
            + COS(RADIANS(s2.start_lat)) * COS(RADIANS(s2.end_lat))
            * COS(RADIANS(s2.end_lon - s2.start_lon))
        )))
      ) / NULLIF(
        6371.0 * ACOS(LEAST(1.0, GREATEST(-1.0,
            SIN(RADIANS(s1.start_lat)) * SIN(RADIANS(s2.end_lat))
            + COS(RADIANS(s1.start_lat)) * COS(RADIANS(s2.end_lat))
            * COS(RADIANS(s2.end_lon - s1.start_lon))
        ))), 0.001) >= 3.0
    GROUP BY s1.trip_id
),

-- 10. 事件严重程度汇总
event_severity AS (
    SELECT trip_id,
        SUM(high_cnt) AS total_high,
        SUM(medium_cnt) AS total_medium,
        SUM(low_cnt) AS total_low,
        SUM(high_cnt + medium_cnt + low_cnt) AS total_events
    FROM (
        SELECT detour_detection.trip_id,
            0 AS high_cnt,
            detour_detection.detour_count AS medium_cnt,
            0 AS low_cnt
        FROM detour_detection WHERE detour_detection.detour_count > 0
        UNION ALL
        SELECT trip_id,
            CASE WHEN stop_seconds_total >= 600 THEN stop_count ELSE 0 END AS high_cnt,
            CASE WHEN stop_seconds_total < 600 THEN stop_count ELSE 0 END AS medium_cnt,
            0 AS low_cnt
        FROM stop_detection WHERE stop_count > 0
        UNION ALL
        SELECT trip_id,
            CASE WHEN speed_jump_count >= 2 THEN speed_jump_count ELSE 0 END AS high_cnt,
            CASE WHEN speed_jump_count < 2 THEN speed_jump_count ELSE 0 END AS medium_cnt,
            0 AS low_cnt
        FROM speed_jump_detection WHERE speed_jump_count > 0
        UNION ALL
        SELECT trip_id,
            0 AS high_cnt,
            jump_point_count AS medium_cnt,
            0 AS low_cnt
        FROM jump_point_detection WHERE jump_point_count > 0
    ) severity_summary
    GROUP BY trip_id
)

-- 最终插入
INSERT INTO tdm_tag_trip_diagnosis (
    trip_id, risk_level, anomaly_score, total_events,
    detour_count, stop_count, speed_jump_count, drift_count, jump_point_count,
    direct_distance_km, actual_distance_km, directness_ratio,
    max_speed_kph, stop_seconds_total, repeated_road_ratio, backtrack_count, compute_time
)
SELECT
    tss.trip_id,
    CASE
        WHEN COALESCE(es.total_high, 0) >= 1 OR COALESCE(es.total_medium, 0) >= 2 THEN 'high'
        WHEN COALESCE(es.total_medium, 0) = 1 OR COALESCE(es.total_events, 0) >= 2 THEN 'medium'
        ELSE 'low'
    END AS risk_level,
    GREATEST(0, 100 - (
        COALESCE(es.total_high, 0) * 35 + COALESCE(es.total_medium, 0) * 20 + COALESCE(es.total_low, 0) * 10
    )) AS anomaly_score,
    COALESCE(es.total_events, 0) AS total_events,
    COALESCE(dt.detour_count, 0),
    COALESCE(sd.stop_count, 0),
    COALESCE(spj.speed_jump_count, 0),
    0 AS drift_count,
    COALESCE(jp.jump_point_count, 0),
    ROUND(COALESCE(dd.direct_distance_km, 0)::NUMERIC, 3),
    ROUND(COALESCE(tss.actual_distance_km, 0)::NUMERIC, 3),
    ROUND((tss.actual_distance_km / NULLIF(dd.direct_distance_km, 0))::NUMERIC, 2),
    ROUND(COALESCE(tss.max_speed_kph, 0)::NUMERIC, 2),
    ROUND(COALESCE(sd.stop_seconds_total, 0)::NUMERIC, 2),
    ROUND(COALESCE(rr.repeated_road_ratio, 0)::NUMERIC, 3),
    COALESCE(bd.backtrack_count, 0),
    NOW()
FROM trip_segment_stats tss
LEFT JOIN direct_distance dd ON tss.trip_id = dd.trip_id
LEFT JOIN detour_detection dt ON tss.trip_id = dt.trip_id
LEFT JOIN stop_detection sd ON tss.trip_id = sd.trip_id
LEFT JOIN speed_jump_detection spj ON tss.trip_id = spj.trip_id
LEFT JOIN jump_point_detection jp ON tss.trip_id = jp.trip_id
LEFT JOIN road_repeat rr ON tss.trip_id = rr.trip_id
LEFT JOIN backtrack_detection bd ON tss.trip_id = bd.trip_id
LEFT JOIN event_severity es ON tss.trip_id = es.trip_id
WHERE tss.trip_id IS NOT NULL
ON CONFLICT (trip_id) DO UPDATE SET
    risk_level = EXCLUDED.risk_level,
    anomaly_score = EXCLUDED.anomaly_score,
    total_events = EXCLUDED.total_events,
    detour_count = EXCLUDED.detour_count,
    stop_count = EXCLUDED.stop_count,
    speed_jump_count = EXCLUDED.speed_jump_count,
    drift_count = EXCLUDED.drift_count,
    jump_point_count = EXCLUDED.jump_point_count,
    direct_distance_km = EXCLUDED.direct_distance_km,
    actual_distance_km = EXCLUDED.actual_distance_km,
    directness_ratio = EXCLUDED.directness_ratio,
    max_speed_kph = EXCLUDED.max_speed_kph,
    stop_seconds_total = EXCLUDED.stop_seconds_total,
    repeated_road_ratio = EXCLUDED.repeated_road_ratio,
    backtrack_count = EXCLUDED.backtrack_count,
    compute_time = NOW();
```

#### tdm_tag_road_congestion_hourly（路段拥堵统计标签表）

**粒度**：一行一个路段×一个小时×一天

**核心职责**：对应 `forecast_xgboost.py` 中路段级别的拥堵强度训练数据

```sql
-- TDM层：路段小时拥堵统计表
DROP TABLE IF EXISTS tdm_tag_road_congestion_hourly;

CREATE TABLE tdm_tag_road_congestion_hourly (
    road_id INTEGER NOT NULL,
    hour INTEGER NOT NULL,
    stat_date DATE NOT NULL,

    -- 拥堵统计
    segment_count INTEGER,
    avg_speed_kph DOUBLE PRECISION,
    avg_intensity DOUBLE PRECISION,
    congestion_count INTEGER,

    compute_time TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (road_id, hour, stat_date)
);

CREATE INDEX idx_tdm_road_hour ON tdm_tag_road_congestion_hourly(road_id, hour);
```

**ETL 填充 SQL**：

```sql
-- 从 DW 层填充路段拥堵统计表
INSERT INTO tdm_tag_road_congestion_hourly (
    road_id, hour, stat_date,
    segment_count, avg_speed_kph, avg_intensity, congestion_count
)
SELECT
    road_id,
    start_hour,
    log_date AS stat_date,
    COUNT(*) AS segment_count,
    AVG(speed_kph) AS avg_speed_kph,
    AVG(congestion_intensity) AS avg_intensity,
    SUM(CASE WHEN is_congested THEN 1 ELSE 0 END) AS congestion_count
FROM dw_fact_road_segment
WHERE speed_kph IS NOT NULL
GROUP BY road_id, start_hour, log_date
ON CONFLICT (road_id, hour, stat_date) DO UPDATE SET
    segment_count = EXCLUDED.segment_count,
    avg_speed_kph = EXCLUDED.avg_speed_kph,
    avg_intensity = EXCLUDED.avg_intensity,
    congestion_count = EXCLUDED.congestion_count,
    compute_time = NOW();
```

#### tdm_feat_road_prediction（预测特征表）

**粒度**：一行一个路段×一个目标小时×一个特征日期

**核心职责**：对应 `forecast_xgboost.py` 中 `_prepare_training_matrices` 的特征结构

**特征列对应关系**：
| 特征列             | `FEATURE_ORDER` 索引 | 说明               |
| ------------------ | -------------------- | ------------------ |
| road_id            | 0                    | 路段ID             |
| target_hour        | 1                    | 目标小时           |
| hour_sin, hour_cos | 2-3                  | 时间循环编码       |
| lag1_intensity     | 4                    | 1小时前拥堵强度    |
| lag2_intensity     | 5                    | 2小时前拥堵强度    |
| lag3_intensity     | 6                    | 3小时前拥堵强度    |
| road_total_count   | 7                    | 路段总样本数       |
| hour_global_mean   | 8                    | 全局小时均值       |
| target_intensity   | -                    | 目标值（训练标签） |

```sql
-- TDM层：路段预测特征表（用于模型训练和推理）
DROP TABLE IF EXISTS tdm_feat_road_prediction;

CREATE TABLE tdm_feat_road_prediction (
    road_id INTEGER NOT NULL,
    target_hour INTEGER NOT NULL,
    feature_date DATE NOT NULL,

    -- 时间循环特征
    hour_sin DOUBLE PRECISION,
    hour_cos DOUBLE PRECISION,

    -- 滞后特征（lag1, lag2, lag3）
    lag1_intensity DOUBLE PRECISION,
    lag2_intensity DOUBLE PRECISION,
    lag3_intensity DOUBLE PRECISION,

    -- 聚合特征
    road_total_count INTEGER,
    hour_global_mean DOUBLE PRECISION,

    -- 目标值（训练时使用）
    target_intensity DOUBLE PRECISION,

    compute_time TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (road_id, target_hour, feature_date)
);

CREATE INDEX idx_tdm_feat_road ON tdm_feat_road_prediction(road_id, target_hour);
```

**ETL 填充 SQL**：

```sql
-- 从路段拥堵统计表计算预测特征
WITH road_hour_stats AS (
    SELECT
        road_id,
        start_hour AS hour,
        AVG(congestion_intensity) AS avg_intensity,
        COUNT(*) AS total_count
    FROM dw_fact_road_segment
    WHERE speed_kph IS NOT NULL
    GROUP BY road_id, start_hour
),
hour_global AS (
    SELECT
        start_hour AS hour,
        AVG(congestion_intensity) AS global_avg
    FROM dw_fact_road_segment
    WHERE speed_kph IS NOT NULL
    GROUP BY start_hour
),
global_avg AS (
    SELECT AVG(congestion_intensity) AS val
    FROM dw_fact_road_segment
    WHERE speed_kph IS NOT NULL
)
INSERT INTO tdm_feat_road_prediction (
    road_id, target_hour, feature_date,
    hour_sin, hour_cos,
    lag1_intensity, lag2_intensity, lag3_intensity,
    road_total_count, hour_global_mean,
    target_intensity
)
SELECT
    r.road_id,
    r.hour AS target_hour,
    CURRENT_DATE AS feature_date,

    -- 时间循环特征（2π * hour / 24）
    SIN(2.0 * PI() * r.hour / 24.0) AS hour_sin,
    COS(2.0 * PI() * r.hour / 24.0) AS hour_cos,

    -- lag1: 1小时前
    COALESCE(r_lag1.avg_intensity, hg_lag1.global_avg, ga.val, 0.5),
    -- lag2: 2小时前
    COALESCE(r_lag2.avg_intensity, hg_lag2.global_avg, ga.val, 0.5),
    -- lag3: 3小时前
    COALESCE(r_lag3.avg_intensity, hg_lag3.global_avg, ga.val, 0.5),

    r.total_count,
    COALESCE(hg.global_avg, ga.val, 0.5),
    r.avg_intensity

FROM road_hour_stats r
LEFT JOIN road_hour_stats r_lag1
    ON r.road_id = r_lag1.road_id AND r_lag1.hour = ((r.hour - 1 + 24) % 24)
LEFT JOIN road_hour_stats r_lag2
    ON r.road_id = r_lag2.road_id AND r_lag2.hour = ((r.hour - 2 + 24) % 24)
LEFT JOIN road_hour_stats r_lag3
    ON r.road_id = r_lag3.road_id AND r_lag3.hour = ((r.hour - 3 + 24) % 24)
LEFT JOIN hour_global hg ON r.hour = hg.hour
LEFT JOIN hour_global hg_lag1 ON hg_lag1.hour = ((r.hour - 1 + 24) % 24)
LEFT JOIN hour_global hg_lag2 ON hg_lag2.hour = ((r.hour - 2 + 24) % 24)
LEFT JOIN hour_global hg_lag3 ON hg_lag3.hour = ((r.hour - 3 + 24) % 24)
CROSS JOIN global_avg ga
ON CONFLICT (road_id, target_hour, feature_date) DO UPDATE SET
    hour_sin = EXCLUDED.hour_sin,
    hour_cos = EXCLUDED.hour_cos,
    lag1_intensity = EXCLUDED.lag1_intensity,
    lag2_intensity = EXCLUDED.lag2_intensity,
    lag3_intensity = EXCLUDED.lag3_intensity,
    road_total_count = EXCLUDED.road_total_count,
    hour_global_mean = EXCLUDED.hour_global_mean,
    target_intensity = EXCLUDED.target_intensity,
    compute_time = NOW();
```

---

## 五、ADS 层（应用数据层）

### 5.1 设计原则

- 面向应用场景：表结构直接对应前端展示需求
- 高性能查询：针对常用查询模式创建复合索引
- 服务化封装：字段命名与API响应的JSON结构一致
- 数据时效性：按业务需求设定不同的刷新频率

### 5.2 表结构

#### ads_car_portrait_summary（车辆画像汇总表）

**粒度**：一行一辆车×一个日期

**对应 API**：`GET /api/cars/{device_id}/portrait` → `CarPortraitSummary`

```sql
-- ADS层：车辆画像汇总表
DROP TABLE IF EXISTS ads_car_portrait_summary;

CREATE TABLE ads_car_portrait_summary (
    device_id VARCHAR(50) NOT NULL,
    computed_date DATE NOT NULL DEFAULT CURRENT_DATE,

    -- CarPortraitSummary 字段
    total_trips INTEGER,
    total_distance_km DOUBLE PRECISION,
    avg_trip_distance_km DOUBLE PRECISION,
    avg_trip_duration_minutes DOUBLE PRECISION,
    active_days INTEGER,
    avg_daily_work_hours DOUBLE PRECISION,
    dominant_shift VARCHAR(20),
    operation_mode VARCHAR(30),
    night_trip_ratio DOUBLE PRECISION,
    hotspot_concentration DOUBLE PRECISION,

    -- peer_group_id
    peer_group_id INTEGER,

    compute_time TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (device_id, computed_date)
);

CREATE INDEX idx_ads_cp_device ON ads_car_portrait_summary(device_id);
```

**ETL 填充 SQL**：

```sql
-- 从 TDM 层填充车辆画像汇总表
INSERT INTO ads_car_portrait_summary (
    device_id, computed_date,
    total_trips, total_distance_km,
    avg_trip_distance_km, avg_trip_duration_minutes,
    active_days, avg_daily_work_hours,
    dominant_shift, operation_mode,
    night_trip_ratio, hotspot_concentration,
    peer_group_id, compute_time
)
SELECT
    device_id, tag_date,
    total_trips, total_distance_km,
    avg_trip_distance_km, avg_trip_duration_minutes,
    active_days, avg_daily_work_hours,
    dominant_shift, operation_mode,
    night_trip_ratio, hotspot_concentration,
    peer_group_id, NOW()
FROM tdm_tag_car_operation
WHERE tag_date = CURRENT_DATE
ON CONFLICT (device_id, computed_date) DO UPDATE SET
    total_trips = EXCLUDED.total_trips,
    total_distance_km = EXCLUDED.total_distance_km,
    avg_trip_distance_km = EXCLUDED.avg_trip_distance_km,
    avg_trip_duration_minutes = EXCLUDED.avg_trip_duration_minutes,
    active_days = EXCLUDED.active_days,
    avg_daily_work_hours = EXCLUDED.avg_daily_work_hours,
    dominant_shift = EXCLUDED.dominant_shift,
    operation_mode = EXCLUDED.operation_mode,
    night_trip_ratio = EXCLUDED.night_trip_ratio,
    hotspot_concentration = EXCLUDED.hotspot_concentration,
    peer_group_id = EXCLUDED.peer_group_id,
    compute_time = NOW();
```

#### ads_car_active_time（车辆活跃时段表）

**粒度**：一行一辆车×一个2小时间隔×一个日期

**对应 API**：`GET /api/cars/{device_id}/portrait` → `ActiveTimeBin[]`

```sql
-- ADS层：车辆活跃时段表
DROP TABLE IF EXISTS ads_car_active_time;

CREATE TABLE ads_car_active_time (
    device_id VARCHAR(50) NOT NULL,
    time_label VARCHAR(5) NOT NULL,
    computed_date DATE NOT NULL DEFAULT CURRENT_DATE,

    trip_count INTEGER,
    distance_km DOUBLE PRECISION,
    share_ratio DOUBLE PRECISION,

    PRIMARY KEY (device_id, time_label, computed_date)
);

CREATE INDEX idx_ads_cat_device ON ads_car_active_time(device_id);
```

**ETL 填充 SQL**：

```sql
-- 从 DW 层展开12个2小时间隔
INSERT INTO ads_car_active_time (device_id, time_label, computed_date, trip_count, distance_km, share_ratio)
SELECT
    c.device_id,
    lbl.label,
    CURRENT_DATE,
    CASE lbl.label
        WHEN '00-02' THEN c.trip_count_0_2 WHEN '02-04' THEN c.trip_count_2_4
        WHEN '04-06' THEN c.trip_count_4_6 WHEN '06-08' THEN c.trip_count_6_8
        WHEN '08-10' THEN c.trip_count_8_10 WHEN '10-12' THEN c.trip_count_10_12
        WHEN '12-14' THEN c.trip_count_12_14 WHEN '14-16' THEN c.trip_count_14_16
        WHEN '16-18' THEN c.trip_count_16_18 WHEN '18-20' THEN c.trip_count_18_20
        WHEN '20-22' THEN c.trip_count_20_22 WHEN '22-24' THEN c.trip_count_22_24
    END,
    CASE lbl.label
        WHEN '00-02' THEN c.distance_0_2 WHEN '02-04' THEN c.distance_2_4
        WHEN '04-06' THEN c.distance_4_6 WHEN '06-08' THEN c.distance_6_8
        WHEN '08-10' THEN c.distance_8_10 WHEN '10-12' THEN c.distance_10_12
        WHEN '12-14' THEN c.distance_12_14 WHEN '14-16' THEN c.distance_14_16
        WHEN '16-18' THEN c.distance_16_18 WHEN '18-20' THEN c.distance_18_20
        WHEN '20-22' THEN c.distance_20_22 WHEN '22-24' THEN c.distance_22_24
    END,
    CASE WHEN c.total_trips > 0
         THEN (CASE lbl.label
            WHEN '00-02' THEN c.trip_count_0_2 WHEN '02-04' THEN c.trip_count_2_4
            WHEN '04-06' THEN c.trip_count_4_6 WHEN '06-08' THEN c.trip_count_6_8
            WHEN '08-10' THEN c.trip_count_8_10 WHEN '10-12' THEN c.trip_count_10_12
            WHEN '12-14' THEN c.trip_count_12_14 WHEN '14-16' THEN c.trip_count_14_16
            WHEN '16-18' THEN c.trip_count_16_18 WHEN '18-20' THEN c.trip_count_18_20
            WHEN '20-22' THEN c.trip_count_20_22 WHEN '22-24' THEN c.trip_count_22_24
         END)::DOUBLE PRECISION / c.total_trips
         ELSE 0
    END
FROM dw_dim_car c
CROSS JOIN (
    VALUES ('00-02'),('02-04'),('04-06'),('06-08'),('08-10'),('10-12'),
           ('12-14'),('14-16'),('16-18'),('18-20'),('20-22'),('22-24')
) AS lbl(label)
ON CONFLICT (device_id, time_label, computed_date) DO UPDATE SET
    trip_count = EXCLUDED.trip_count,
    distance_km = EXCLUDED.distance_km,
    share_ratio = EXCLUDED.share_ratio;
```

#### ads_anomaly_trip（行程异常详情表）

**粒度**：一行一个行程

**对应 API**：`GET /api/trips/{id}/diagnosis` → `TripDiagnosisResponse`

```sql
-- ADS层：行程异常详情表
DROP TABLE IF EXISTS ads_anomaly_trip;

CREATE TABLE ads_anomaly_trip (
    trip_id INTEGER PRIMARY KEY,

    -- TripDiagnosisSummary
    risk_level VARCHAR(10),
    anomaly_score INTEGER,
    total_events INTEGER,

    -- TripDiagnosisMetrics
    direct_distance_km DOUBLE PRECISION,
    actual_distance_km DOUBLE PRECISION,
    directness_ratio DOUBLE PRECISION,
    max_speed_kph DOUBLE PRECISION,
    stop_seconds_total DOUBLE PRECISION,
    repeated_road_ratio DOUBLE PRECISION,
    backtrack_count INTEGER,

    -- 五种异常计数
    detour_count INTEGER DEFAULT 0,
    stop_count INTEGER DEFAULT 0,
    speed_jump_count INTEGER DEFAULT 0,
    drift_count INTEGER DEFAULT 0,
    jump_point_count INTEGER DEFAULT 0,

    compute_time TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_ads_anomaly_risk ON ads_anomaly_trip(risk_level);
CREATE INDEX idx_ads_anomaly_score ON ads_anomaly_trip(anomaly_score);
```

**ETL 填充 SQL**：

```sql
-- 从 TDM 层填充行程异常详情表
INSERT INTO ads_anomaly_trip (
    trip_id, risk_level, anomaly_score, total_events,
    direct_distance_km, actual_distance_km, directness_ratio,
    max_speed_kph, stop_seconds_total, repeated_road_ratio, backtrack_count,
    detour_count, stop_count, speed_jump_count, drift_count, jump_point_count, compute_time
)
SELECT
    trip_id, risk_level, anomaly_score, total_events,
    direct_distance_km, actual_distance_km, directness_ratio,
    max_speed_kph, stop_seconds_total, repeated_road_ratio, backtrack_count,
    detour_count, stop_count, speed_jump_count, drift_count, jump_point_count, NOW()
FROM tdm_tag_trip_diagnosis
WHERE diagnosis_date = CURRENT_DATE
ON CONFLICT (trip_id) DO UPDATE SET
    risk_level = EXCLUDED.risk_level,
    anomaly_score = EXCLUDED.anomaly_score,
    total_events = EXCLUDED.total_events,
    direct_distance_km = EXCLUDED.direct_distance_km,
    actual_distance_km = EXCLUDED.actual_distance_km,
    directness_ratio = EXCLUDED.directness_ratio,
    max_speed_kph = EXCLUDED.max_speed_kph,
    stop_seconds_total = EXCLUDED.stop_seconds_total,
    repeated_road_ratio = EXCLUDED.repeated_road_ratio,
    backtrack_count = EXCLUDED.backtrack_count,
    detour_count = EXCLUDED.detour_count,
    stop_count = EXCLUDED.stop_count,
    speed_jump_count = EXCLUDED.speed_jump_count,
    drift_count = EXCLUDED.drift_count,
    jump_point_count = EXCLUDED.jump_point_count,
    compute_time = NOW();
```

#### ads_forecast_speed（路段速度预测结果表）

**粒度**：一行一个行程×一个预测偏移分钟×一个预测日期

**对应 API**：`GET /api/forecast/speed/by-trip` → `ForecastTripSpeedResponse`

```sql
-- ADS层：路段速度预测结果表
DROP TABLE IF EXISTS ads_forecast_speed;

CREATE TABLE ads_forecast_speed (
    trip_id INTEGER NOT NULL,
    offset_minutes INTEGER NOT NULL,
    forecast_date DATE NOT NULL DEFAULT CURRENT_DATE,

    predicted_speed_kph DOUBLE PRECISION,
    predicted_intensity DOUBLE PRECISION,
    risk_level VARCHAR(10),

    compute_time TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (trip_id, offset_minutes, forecast_date)
);

CREATE INDEX idx_ads_fs_trip ON ads_forecast_speed(trip_id);
```

**ETL 填充 SQL**：

```sql
-- 从 DW 层和 TDM 特征表计算速度预测
INSERT INTO ads_forecast_speed (
    trip_id, offset_minutes, forecast_date,
    predicted_speed_kph, predicted_intensity, risk_level, compute_time
)
SELECT DISTINCT ON (s.trip_id, ((f.target_hour - s.start_hour + 24) % 24) * 60, f.feature_date)
    s.trip_id,
    ((f.target_hour - s.start_hour + 24) % 24) * 60 AS offset_minutes,
    f.feature_date AS forecast_date,
    -- 对应 _speed_from_intensity：min_kph=5.0, max_kph=48.0
    GREATEST(5.0, LEAST(48.0,
        5.0 + 43.0 * POWER(GREATEST(0.0, 1.0 - f.lag1_intensity), 1.25)
    )) AS predicted_speed_kph,
    ROUND(f.lag1_intensity::NUMERIC, 4) AS predicted_intensity,
    -- 对应 _risk_level_from_intensity
    CASE
        WHEN f.lag1_intensity >= 0.65 THEN 'high'
        WHEN f.lag1_intensity >= 0.35 THEN 'medium'
        ELSE 'low'
    END AS risk_level,
    NOW()
FROM dw_fact_road_segment s
JOIN tdm_feat_road_prediction f
    ON s.road_id = f.road_id
    AND s.start_hour = f.target_hour
    AND f.feature_date = CURRENT_DATE
ORDER BY s.trip_id, ((f.target_hour - s.start_hour + 24) % 24) * 60, f.feature_date
ON CONFLICT (trip_id, offset_minutes, forecast_date) DO UPDATE SET
    predicted_speed_kph = EXCLUDED.predicted_speed_kph,
    predicted_intensity = EXCLUDED.predicted_intensity,
    risk_level = EXCLUDED.risk_level,
    compute_time = NOW();
```

#### ads_forecast_demand（区域热度预测结果表）

**粒度**：一行一个行程×一个轨迹点索引×一个预测日期

**对应 API**：`GET /api/forecast/heatmap/by-trip` → `ForecastTripHeatmapResponse`

```sql
-- ADS层：区域热度预测结果表
DROP TABLE IF EXISTS ads_forecast_demand;

CREATE TABLE ads_forecast_demand (
    trip_id INTEGER NOT NULL,
    point_index INTEGER NOT NULL,
    forecast_date DATE NOT NULL DEFAULT CURRENT_DATE,

    lon DOUBLE PRECISION,
    lat DOUBLE PRECISION,
    predicted_trips DOUBLE PRECISION,
    intensity DOUBLE PRECISION,
    sample_count INTEGER,

    compute_time TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (trip_id, point_index, forecast_date)
);

CREATE INDEX idx_ads_fd_trip ON ads_forecast_demand(trip_id);
```

**ETL 填充 SQL**：

```sql
-- 从 DW 层和 TDM 特征表计算热力预测
INSERT INTO ads_forecast_demand (
    trip_id,
    point_index,
    forecast_date,
    lon,
    lat,
    predicted_trips,
    intensity,
    sample_count,
    compute_time
)
SELECT DISTINCT ON (s.trip_id, s.segment_index, f.feature_date)
    s.trip_id,
    s.segment_index AS point_index,
    f.feature_date AS forecast_date,
    s.start_lon AS lon,
    s.start_lat AS lat,

    -- predicted_trips = intensity * sample_count / 24，最低 0.01
    GREATEST(0.01,
        f.lag1_intensity * GREATEST(COALESCE(f.road_total_count, 0) / 24.0, 1.0)
    ) AS predicted_trips,

    ROUND(f.lag1_intensity::NUMERIC, 4) AS intensity,
    COALESCE(f.road_total_count, 0) AS sample_count,

    NOW()
FROM dw_fact_road_segment s
JOIN tdm_feat_road_prediction f
    ON s.road_id = f.road_id
    AND s.start_hour = f.target_hour
    AND f.feature_date = CURRENT_DATE
WHERE s.start_lon IS NOT NULL
  AND s.start_lat IS NOT NULL
ORDER BY s.trip_id, s.segment_index, f.feature_date
ON CONFLICT (trip_id, point_index, forecast_date) DO UPDATE SET
    lon = EXCLUDED.lon,
    lat = EXCLUDED.lat,
    predicted_trips = EXCLUDED.predicted_trips,
    intensity = EXCLUDED.intensity,
    sample_count = EXCLUDED.sample_count,
    compute_time = NOW();
```
