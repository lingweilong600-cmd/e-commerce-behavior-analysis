# 电商用户行为分析数仓（PySpark + Hive）

基于 REES46 多品类电商行为数据（Kaggle `2019-Nov.csv`，约 9GB），
使用 PySpark + Hive 搭建 **ODS → DWD → DWS → ADS** 四层离线数仓，
输出流量、行为分布、商品/品类排行、转化漏斗等核心指标。

## 技术栈

| 组件 | 说明 |
|---|---|
| PySpark | 本地模式（local[*]）连远程集群，ETL 计算引擎 |
| Hive | 数仓表管理，元数据存 MySQL |
| HDFS | 存储，namenode：node1:8020 |
| Hive metastore | thrift 服务，node1:9083 |
| 存储格式 | Parquet 列式存储，所有表按业务日期 dt（yyyy-MM-dd）分区 |

## 数仓分层

| 层 | 表 | 说明 |
|---|---|---|
| ODS 贴源层 | ods_user_behavior | 原始数据原样落地，字段全 STRING，保证可回溯 |
| DWD 明细层 | dwd_user_behavior | 清洗：去重、过滤异常、缺失补全、类型转换 |
| DWS 汇总层 | dws_user_behavior_1d<br>dws_product_behavior_1d<br>dws_category_behavior_1d | 按天 × 用户/商品/品类 三个主题轻度汇总 |
| ADS 指标层 | ads_traffic_metrics<br>ads_behavior_distribution<br>ads_product_topn<br>ads_category_topn<br>ads_conversion_funnel | 最终指标：PV/UV、行为分布、TOP10、转化漏斗 |

## 目录结构

```
e-commerce-behavior-analysis/
├── sql/
│   └── create_tables.sql      # 全部分层表 DDL（可在 beeline 里直接执行）
├── scripts/
│   ├── read_data.py           # 阶段二：读数据、探索结构
│   ├── load_ods.py            # 阶段四(1)：原始 CSV -> ODS 表
│   ├── load_dwd.py            # 阶段四(2)：ODS -> DWD（清洗）
│   ├── load_dws.py            # 阶段五(1)：DWD -> DWS 汇总
│   └── load_ads.py            # 阶段五(2)：DWS -> ADS 指标
└── conf/
    └── hive-site.xml          # 可选：连接元数据的参考配置（脚本已内联配置，可不用）
```

## 运行步骤

按依赖顺序执行（每个脚本可独立重跑，INSERT OVERWRITE 只覆盖写入的那几天）：

```
python scripts/load_ods.py     # 499,999 行进 ODS
python scripts/load_dwd.py     # 清洗后 499,201 行进 DWD
python scripts/load_dws.py     # 三张汇总表
python scripts/load_ads.py     # 五张指标表
```

连接 Hive 的配置已内联在每个脚本的 SparkSession 里：

```python
.config("spark.sql.warehouse.dir", "hdfs://node1:8020/user/hive/warehouse") \
.config("hive.metastore.uris", "thrift://node1:9083") \
```

## 核心指标口径

- **流量**：PV = 总记录数；UV = 去重 user_id（即当天日活 DAU）；人均行为次数 = PV / UV
- **转化漏斗**：view → cart → buy，**必须用去重用户数**（不用行为次数），
  各环节 rate 统一除以首环节（浏览）用户数：cart 行 = 加购率，buy 行 = 整体转化率
- **TOP10**：ROW_NUMBER() OVER(PARTITION BY dt ORDER BY cnt DESC)，每个 dt 分区内各自排名

## 数据说明

1. 数据集只包含 view / cart / purchase 三种行为，没有 remove_from_cart；
2. **purchase 次数(9595) > cart 次数(7553)**：若按行为次数计算转化率，漏斗会失真，
   因此转化漏斗采用去重用户数口径；
3. category_id 数值极大（如 2053013555631882655），必须用 BIGINT，IntType 会溢出；
4. 样本 sample_500k.csv 截取自全量文件开头的 50 万行，全部落在 2019-11-01，
   故只有一个分区；接入全量数据后 dt 覆盖 11-01 ~ 11-30 共 30 个分区。
