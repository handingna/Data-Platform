```sql
DROP TABLE IF EXISTS car; -- 如果表已存在则删除重来

CREATE TABLE car (
    device_id VARCHAR(50) PRIMARY KEY,
    trip_ids INTEGER[],                 -- 行程 ID 数组
    trips_distance FLOAT[],             -- 每一个行程对应的里程数数组
    total_distance FLOAT DEFAULT 0,      -- 该车总里程数
    trips_total INTEGER DEFAULT 0,       -- 总行程数
    
    -- 每个时间段的行程数统计 (Count)
    trips_total_0_2 INTEGER DEFAULT 0,
    trips_total_2_4 INTEGER DEFAULT 0,
    trips_total_4_6 INTEGER DEFAULT 0,
    trips_total_6_8 INTEGER DEFAULT 0,
    trips_total_8_10 INTEGER DEFAULT 0,
    trips_total_10_12 INTEGER DEFAULT 0,
    trips_total_12_14 INTEGER DEFAULT 0,
    trips_total_14_16 INTEGER DEFAULT 0,
    trips_total_16_18 INTEGER DEFAULT 0,
    trips_total_18_20 INTEGER DEFAULT 0,
    trips_total_20_22 INTEGER DEFAULT 0,
    trips_total_22_24 INTEGER DEFAULT 0,

    -- 每个时间段的里程数统计 (Sum)
    total_distance_0_2 FLOAT DEFAULT 0,
    total_distance_2_4 FLOAT DEFAULT 0,
    total_distance_4_6 FLOAT DEFAULT 0,
    total_distance_6_8 FLOAT DEFAULT 0,
    total_distance_8_10 FLOAT DEFAULT 0,
    total_distance_10_12 FLOAT DEFAULT 0,
    total_distance_12_14 FLOAT DEFAULT 0,
    total_distance_14_16 FLOAT DEFAULT 0,
    total_distance_16_18 FLOAT DEFAULT 0,
    total_distance_18_20 FLOAT DEFAULT 0,
    total_distance_20_22 FLOAT DEFAULT 0,
    total_distance_22_24 FLOAT DEFAULT 0
);
```

```sql
INSERT INTO car (
    device_id, 
    trip_ids, 
    trips_distance,
    total_distance,
    trips_total,
    -- 行程数统计字段
    trips_total_0_2, trips_total_2_4, trips_total_4_6, trips_total_6_8, 
    trips_total_8_10, trips_total_10_12, trips_total_12_14, trips_total_14_16, 
    trips_total_16_18, trips_total_18_20, trips_total_20_22, trips_total_22_24,
    -- 里程数统计字段
    total_distance_0_2, total_distance_2_4, total_distance_4_6, total_distance_6_8, 
    total_distance_8_10, total_distance_10_12, total_distance_12_14, total_distance_14_16, 
    total_distance_16_18, total_distance_18_20, total_distance_20_22, total_distance_22_24
)
SELECT 
    devid,
    -- 1. 聚合数组
    array_agg(trip_id ORDER BY start_time),
    array_agg(distance_km ORDER BY start_time),
    -- 2. 总体统计
    SUM(distance_km),
    COUNT(trip_id),
    
    -- 3. 分时段行程数 (Count)
    COUNT(trip_id) FILTER (WHERE EXTRACT(HOUR FROM start_time::timestamp) >= 0  AND EXTRACT(HOUR FROM start_time::timestamp) < 2),
    COUNT(trip_id) FILTER (WHERE EXTRACT(HOUR FROM start_time::timestamp) >= 2  AND EXTRACT(HOUR FROM start_time::timestamp) < 4),
    COUNT(trip_id) FILTER (WHERE EXTRACT(HOUR FROM start_time::timestamp) >= 4  AND EXTRACT(HOUR FROM start_time::timestamp) < 6),
    COUNT(trip_id) FILTER (WHERE EXTRACT(HOUR FROM start_time::timestamp) >= 6  AND EXTRACT(HOUR FROM start_time::timestamp) < 8),
    COUNT(trip_id) FILTER (WHERE EXTRACT(HOUR FROM start_time::timestamp) >= 8  AND EXTRACT(HOUR FROM start_time::timestamp) < 10),
    COUNT(trip_id) FILTER (WHERE EXTRACT(HOUR FROM start_time::timestamp) >= 10 AND EXTRACT(HOUR FROM start_time::timestamp) < 12),
    COUNT(trip_id) FILTER (WHERE EXTRACT(HOUR FROM start_time::timestamp) >= 12 AND EXTRACT(HOUR FROM start_time::timestamp) < 14),
    COUNT(trip_id) FILTER (WHERE EXTRACT(HOUR FROM start_time::timestamp) >= 14 AND EXTRACT(HOUR FROM start_time::timestamp) < 16),
    COUNT(trip_id) FILTER (WHERE EXTRACT(HOUR FROM start_time::timestamp) >= 16 AND EXTRACT(HOUR FROM start_time::timestamp) < 18),
    COUNT(trip_id) FILTER (WHERE EXTRACT(HOUR FROM start_time::timestamp) >= 18 AND EXTRACT(HOUR FROM start_time::timestamp) < 20),
    COUNT(trip_id) FILTER (WHERE EXTRACT(HOUR FROM start_time::timestamp) >= 20 AND EXTRACT(HOUR FROM start_time::timestamp) < 22),
    COUNT(trip_id) FILTER (WHERE EXTRACT(HOUR FROM start_time::timestamp) >= 22 AND EXTRACT(HOUR FROM start_time::timestamp) < 24),

    -- 4. 分时段里程数 (Sum)
    COALESCE(SUM(distance_km) FILTER (WHERE EXTRACT(HOUR FROM start_time::timestamp) >= 0  AND EXTRACT(HOUR FROM start_time::timestamp) < 2), 0),
    COALESCE(SUM(distance_km) FILTER (WHERE EXTRACT(HOUR FROM start_time::timestamp) >= 2  AND EXTRACT(HOUR FROM start_time::timestamp) < 4), 0),
    COALESCE(SUM(distance_km) FILTER (WHERE EXTRACT(HOUR FROM start_time::timestamp) >= 4  AND EXTRACT(HOUR FROM start_time::timestamp) < 6), 0),
    COALESCE(SUM(distance_km) FILTER (WHERE EXTRACT(HOUR FROM start_time::timestamp) >= 6  AND EXTRACT(HOUR FROM start_time::timestamp) < 8), 0),
    COALESCE(SUM(distance_km) FILTER (WHERE EXTRACT(HOUR FROM start_time::timestamp) >= 8  AND EXTRACT(HOUR FROM start_time::timestamp) < 10), 0),
    COALESCE(SUM(distance_km) FILTER (WHERE EXTRACT(HOUR FROM start_time::timestamp) >= 10 AND EXTRACT(HOUR FROM start_time::timestamp) < 12), 0),
    COALESCE(SUM(distance_km) FILTER (WHERE EXTRACT(HOUR FROM start_time::timestamp) >= 12 AND EXTRACT(HOUR FROM start_time::timestamp) < 14), 0),
    COALESCE(SUM(distance_km) FILTER (WHERE EXTRACT(HOUR FROM start_time::timestamp) >= 14 AND EXTRACT(HOUR FROM start_time::timestamp) < 16), 0),
    COALESCE(SUM(distance_km) FILTER (WHERE EXTRACT(HOUR FROM start_time::timestamp) >= 16 AND EXTRACT(HOUR FROM start_time::timestamp) < 18), 0),
    COALESCE(SUM(distance_km) FILTER (WHERE EXTRACT(HOUR FROM start_time::timestamp) >= 18 AND EXTRACT(HOUR FROM start_time::timestamp) < 20), 0),
    COALESCE(SUM(distance_km) FILTER (WHERE EXTRACT(HOUR FROM start_time::timestamp) >= 20 AND EXTRACT(HOUR FROM start_time::timestamp) < 22), 0),
    COALESCE(SUM(distance_km) FILTER (WHERE EXTRACT(HOUR FROM start_time::timestamp) >= 22 AND EXTRACT(HOUR FROM start_time::timestamp) < 24), 0)
FROM trip_data
GROUP BY devid;
```

```sql

-- 如果表已存在，建议删除重来以确保字段顺序一致
DROP TABLE IF EXISTS trip_data CASCADE;

CREATE TABLE trip_data (
    -- 1. 原始 CSV 对应的列 (必须与 CSV 列顺序或 \copy 指定顺序一致)
    devid VARCHAR(50),                 -- 车辆 ID
    lon FLOAT[],                       -- 经度数组
    lat FLOAT[],                       -- 纬度数组
    tms BIGINT[],                      -- Unix 时间戳数组
    roads INT[],                       -- 道路 ID 数组
    time FLOAT[],                    -- 原始行程时间数组 (CSV 中的 time 列)
    frac FLOAT[],                      -- 行程分数/进度数组
    route INT[],                       -- 路径 ID 数组
    route_heading INT[],               -- 航向角数组
    log_date DATE NOT NULL,            -- 日期 (分区键)

    -- 2. 数据库生成的自增 ID 和 衍生计算列
    trip_id SERIAL,                    -- 行程自增 ID
    distance_km FLOAT,                 -- 总里程 (待计算)
    duration INTERVAL,                 -- 总耗时 (待计算)
    start_time TIMESTAMP,              -- 开始时间 (待计算)
    end_time TIMESTAMP,                -- 结束时间 (待计算)
    speed_array FLOAT[],               -- 瞬时速度数组 (待计算)

    PRIMARY KEY (trip_id, log_date)
) PARTITION BY RANGE (log_date);

-- 为 2015年1月3日 创建分区子表
CREATE TABLE trip_data_20150103 PARTITION OF trip_data
    FOR VALUES FROM ('2015-01-03') TO ('2015-01-04');

-- 如果还有其他日期，以此类推
CREATE TABLE trip_data_20150104 PARTITION OF trip_data
    FOR VALUES FROM ('2015-01-04') TO ('2015-01-05');

-- psql执行
\copy trip_data (devid, lon, lat, tms, roads, time, frac, route, route_heading, log_date) FROM 'D:/Desktop/TheaTang/code/Data_Platform/trip_data_correct.csv' WITH (FORMAT csv, HEADER true, QUOTE '"', DELIMITER ',');


WITH trip_calculations AS (
    SELECT 
        trip_id,
        log_date,
        -- 计算总里程 (km)
        ST_Length(
            ST_SetSRID(
                ST_MakeLine(
                    ARRAY(
                        SELECT ST_MakePoint(lon[i], lat[i])
                        FROM generate_series(1, array_upper(lon, 1)) AS i
                    )
                ), 4326
            )::geography
        ) / 1000.0 AS calc_dist,
        -- 计算时间
        to_timestamp(tms[1]) AS calc_start,
        to_timestamp(tms[array_upper(tms, 1)]) AS calc_end,
        to_timestamp(tms[array_upper(tms, 1)]) - to_timestamp(tms[1]) AS calc_duration,
        -- 计算速度数组
        (
            SELECT array_agg(s.v ORDER BY s.idx) 
            FROM (
                SELECT 
                    idx,
                    CASE 
                        WHEN (tms[idx] - tms[idx-1]) > 0 THEN 
                            ROUND((ST_Distance(
                                ST_MakePoint(lon[idx], lat[idx])::geography, 
                                ST_MakePoint(lon[idx-1], lat[idx-1])::geography
                            ) / (tms[idx] - tms[idx-1]) * 3.6)::numeric, 2)
                        ELSE 0 
                    END AS v
                FROM generate_series(2, array_upper(lon, 1)) AS idx
            ) s
        ) AS calc_speeds
    FROM trip_data
    WHERE array_upper(lon, 1) > 1
)
UPDATE trip_data t
SET 
    distance_km = c.calc_dist,
    duration = c.calc_duration,
    start_time = c.calc_start,
    end_time = c.calc_end,
    speed_array = c.calc_speeds
FROM trip_calculations c
WHERE t.trip_id = c.trip_id AND t.log_date = c.log_date;
```
