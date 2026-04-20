# 出租车异常行程诊断与需求洞察平台功能与数据中台层级对应

本文件详细说明当前已实现的三大平台功能：
- 城市出租车异常行程诊断平台（异常检测、异常排行、异常路段分布）
- 城市出租车需求洞察平台（上车/下车热区、分时段热力、热点迁移分析）
- 出租车运营画像平台（活跃时段分析、常驻区域、运营模式画像）

同时说明这些功能在交通数据中台各层（ODS、DW、ADS）之间的对应关系，便于后续报告中清晰展示数据加工与服务链路。

## 1. ODS 原始层

### 数据来源
- 主要来源于后端数据库表：`public.trip_data`。
- 另外，`public.car` 表也是核心原始表，支撑运营画像平台数据来源。
- `trip_data` 核心字段包括：
  - 起点经纬度：`lon[1]`、`lat[1]`
  - 终点经纬度：`lon[array_length(lon,1)]`、`lat[array_length(lat,1)]`
  - 起始时间：`start_time`
  - 结束时间：`end_time`
  - 轨迹点序列：`lon`、`lat`、`tms`、`roads`
- `car` 表核心字段包括：
  - 车辆 ID：`device_id`
  - 归属行程列表：`trip_ids`
  - 行程里程数组：`trips_distance`
  - 总里程：`total_distance`
  - 行程总数：`trips_total`
  - 分时段行程计数：`trips_total_0_2`、`trips_total_2_4`、…、`trips_total_22_24`
  - 分时段里程统计：`total_distance_0_2`、`total_distance_2_4`、…、`total_distance_22_24`

### 已实现功能
- 读取每条行程的起终点坐标与时间，作为需求热力分析的基础数据。
- 读取轨迹点序列、时间戳、速度、路网匹配结果，用于异常行程诊断和异常路段分析。
- 支持按时间范围筛选行程起点/终点，确保分析聚焦到用户指定时段。
- 通过 `array_length(lon,1) >= 1` 保障只处理有效行程数据。

### 说明
ODS 层定位为“原始行程事件的采集与存储”，本项目已将其作为两大业务的第一手数据源：
- 需求洞察平台基于真实上车/下车事件统计热点；
- 异常行程诊断平台基于轨迹与速度数据检测绕行、异常停留、速度突变、跳点等异常行为。

## 2. DW 加工层

### 处理目标
将原始打车事件数据转换为可视化的区域热度、时间热度和迁移趋势指标。

### 已实现数据加工结构
- **异常行程诊断加工**
  - 通过 `backend/app/services.py` 中 `analyze_trip_diagnosis` 处理单条行程轨迹，检测绕行、停留、速度突变、漂移、跳点等异常事件。
  - 生成异常事件统计、风险评分、异常路段分布等结构化结果，为异常诊断平台提供基础数据。
- **网格化区域聚合**
  - 通过 `backend/app/services.py` 中 `_grid_cell`、`_grid_center`、`_grid_bounds` 函数，将坐标映射到固定网格。
  - 每个网格输出：区域编号、中心点、边界坐标、上车数量、下车数量、总热度。
- **热点区域输出**
  - 以 `DemandHotspotItem` 结构输出：`zone_id`、`demand_type`、`trip_count`、`pickup_count`、`dropoff_count`、`avg_hour`、`center`、`bounds`。
  - 该结构在 `backend/app/schemas.py` 中定义，为前端地图展示提供完整要素。
- **时间分桶统计**
  - 使用 2 小时为间隔的时间段统计上车、下车、总量。
  - 结果返回 `time_buckets`，用于前端绘制分时段热力图。
- **热点迁移分析**
  - 将用户选择的时间范围分成“前半段 / 后半段”，按时间段长度平均拆分。
  - 计算每个网格在前后半段的热度排名和次数，并生成迁移趋势指标。
  - 结果输出包含：`early_rank`、`late_rank`、`early_count`、`late_count`、`trend`、`rank_change`。
- **运营画像特征聚合**
  - 通过 `backend/app/car_portrait.py` 对单车行程进行聚合分析，计算活跃时间段、日运行节奏、平均距离、热点区域、运营模式等画像指标。
  - 结果返回车辆画像结构，用于运营画像页面展示车辆运行特征和同类对比。

### 说明
DW 层是本项目的核心加工层，将原始点数据转化为“区域范围热度”、“时段热度”和“热点迁移变化”三个分析维度，是业务分析输出的基础。

## 3. ADS 服务层

### 服务接口
- 后端暴露接口：`GET /api/demand/hotspots`
- 该接口由 `backend/app/main.py` 中的 `get_demand_hotspots` 定义，调用 `fetch_demand_hotspots`。

### 接口输出
- `sample_trip_count`：本次分析使用的样本行程数。
- `hotspot_count`：返回的热点区域数量。
- `items`：热点区域列表，每项包含网格范围和热度信息。
- `time_buckets`：分时段统计结果。
- `migration_analysis`：热点迁移分析结果。

### 前端呈现
- 页面入口：`src/views/DemandView.vue`
- 地图组件：`src/components/AmapDemandMap.vue`
  - 使用高德地图绘制网格多边形，避免单点热力，增强区域感。
  - 根据 `demand_type` 采用不同颜色区分上车、下车、混合热点。
- 图表组件：ECharts 绘制分时段热力柱状图与折线图。
- 迁移分析展示：默认展示前 5 条迁移结果，支持展开查看更多。

### 说明
ADS 层负责将 DW 层加工结果封装成业务接口，并由前端直接消费，实现“可视化分析页面”效果。当前已实现的功能包括热点地图、时间热力趋势和迁移变化展示。

## 4. 功能实现与中台层级对应总结

| 数据中台层 | 本项目功能 | 实现位置 | 说明 |
| --- | --- | --- | --- |
| ODS 原始层 | 行程起终点、时间、轨迹原始数据读取 | `public.trip_data`、`backend/app/services.py` | 作为需求和异常分析的原始事件数据来源。 |
| DW 加工层 | 异常诊断事件提取 | `backend/app/services.py::analyze_trip_diagnosis` | 生成绕行、停留、速度异常、跳点等结构化异常结果。 |
| DW 加工层 | 网格热区聚合 | `backend/app/services.py::_grid_cell/_grid_bounds` | 将点位聚合为更稳定的区域热点。 |
| DW 加工层 | 时间分段统计 | `backend/app/services.py::_time_bins`, `time_buckets` | 生成 2 小时段上车/下车热度。 |
| DW 加工层 | 热点迁移趋势 | `backend/app/services.py::_compute_migration_trends` | 比较前后半段排名变化，输出迁移趋势。 |
| ADS 服务层 | 需求洞察接口 | `backend/app/main.py`、`/api/demand/hotspots` | 提供前端可直接消费的聚合结果。 |
| ADS 服务层 | 异常诊断接口 | `backend/app/main.py`、`/api/anomaly/vehicles`、`/api/anomaly/roads`、`/api/trips/{trip_id}/diagnosis` | 提供异常行程分析与车辆/路段排行。 |
| ADS 服务层 | 运营画像接口 | `backend/app/main.py`、`/api/cars/{device_id}/portrait`、`/api/cars/{device_id}` | 提供车辆运营画像查询与行程列表展示。 |
| ADS 服务层 | 地图与图表展示 | `src/views/DemandView.vue`, `src/components/AmapDemandMap.vue`, `src/views/TripView.vue`, `src/views/CarView.vue` | 业务端展示“需求洞察平台”、“异常诊断平台”和“运营画像平台”。 |

## 5. 报告推荐表述

- “本项目基于 ODS 原始行程数据，读取出租车起终点与时间信息，确保需求分析源于真实打车事件。"
- “在 DW 层，我们将原始坐标映射到固定栅格，生成区域范围热力区，而不是噪声更大的点热点。"
- “同时，系统按 2 小时为单位统计上车/下车数量，支持分时段热力趋势分析，帮助识别出行高峰与时间结构。"
- “热点迁移分析通过比较所选时间段内前后两半段的网格排名变化，揭示需求热度在空间上的转移趋势。"
- “ADS 层则提供统一接口与前端可视化页面，最终呈现‘上车热区、下车热区、分时段热力图、热点迁移分析’等业务能力。"
- “异常诊断平台通过轨迹分析与事件检测，提供异常车辆排行和异常路段分布，支撑运营质量监控与风险预警。"

## 6. 已覆盖功能清单

- 上车热区、下车热区、混合需求热点区域可视化
- 区域热点以网格范围输出，避免精确点噪声
- 分时段热力趋势分析（2 小时区间）
- 热点迁移分析（前半段 vs 后半段排名变化）
- 异常行程诊断平台：异常轨迹诊断、异常事件标注、异常车辆排行、异常路段分布
- 出租车运营画像平台：活跃时间分析、常驻区域特征、运营模式画像、车辆画像查询
- 轨迹诊断接口与异常报告输出，支持单程诊断和异常指标展示

