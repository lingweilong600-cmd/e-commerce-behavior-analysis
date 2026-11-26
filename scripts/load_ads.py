# -*- coding: utf-8 -*-
# 第五阶段(2)：DWS -> ADS 指标层
# 最终展示的指标表：
#   ads_traffic_metrics        流量指标：PV / UV(即日活) / 人均行为次数
#   ads_behavior_distribution  行为类型分布及占比
#   ads_product_topn           商品浏览/加购/购买 TOP10
#   ads_category_topn          品类访问 TOP10
#   ads_conversion_funnel      转化漏斗（注意：去重用户数口径）

import os
os.environ["HADOOP_USER_NAME"] = "root"  # 解决 HDFS 权限

from pyspark.sql import SparkSession

# ---------- 1. 创建 SparkSession，开启 Hive ----------
spark = SparkSession.builder \
    .appName("load_ads") \
    .master("local[*]") \
    .config("spark.sql.warehouse.dir", "hdfs://node1:8020/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://node1:9083") \
    .config("hive.exec.dynamic.partition", "true") \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("USE ecommerce_db")

# ---------- 2. 建五张 ADS 表（IF NOT EXISTS，重复执行不报错） ----------
spark.sql("""
    CREATE TABLE IF NOT EXISTS ads_traffic_metrics (
        pv            BIGINT COMMENT 'PV(总记录数)',
        uv            BIGINT COMMENT 'UV(去重用户数，即当天日活DAU)',
        avg_behavior  DOUBLE COMMENT '人均行为次数 = pv / uv'
    )
    COMMENT '流量指标结果表'
    PARTITIONED BY (dt STRING)
    STORED AS PARQUET
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS ads_behavior_distribution (
        event_type    STRING COMMENT '行为类型',
        cnt           BIGINT COMMENT '记录数',
        ratio         DOUBLE COMMENT '占比(小数，如 0.96 表示 96%)'
    )
    COMMENT '行为类型分布结果表'
    PARTITIONED BY (dt STRING)
    STORED AS PARQUET
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS ads_product_topn (
        product_id    BIGINT COMMENT '商品ID',
        metric_type   STRING COMMENT '排行类型: view / cart / buy',
        cnt           BIGINT COMMENT '次数',
        rank          INT    COMMENT '排名(1-10)'
    )
    COMMENT '商品TOP10结果表'
    PARTITIONED BY (dt STRING)
    STORED AS PARQUET
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS ads_category_topn (
        category_id   BIGINT COMMENT '品类ID',
        category_code STRING COMMENT '品类代码',
        view_cnt      BIGINT COMMENT '访问次数',
        rank          INT    COMMENT '排名'
    )
    COMMENT '品类访问TOP10结果表'
    PARTITIONED BY (dt STRING)
    STORED AS PARQUET
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS ads_conversion_funnel (
        step          STRING COMMENT '漏斗环节: view / cart / buy',
        user_cnt      BIGINT COMMENT '该环节去重用户数',
        rate          DOUBLE COMMENT '相对首环节(浏览)的转化率，小数形式'
    )
    COMMENT '转化漏斗结果表'
    PARTITIONED BY (dt STRING)
    STORED AS PARQUET
""")

# ---------- 3. 写数据 ----------

# 3.1 流量指标：从用户汇总表算（PV=总行为次数，UV=去重用户数）
spark.sql("""
    INSERT OVERWRITE TABLE ecommerce_db.ads_traffic_metrics PARTITION(dt)
    SELECT
        SUM(behavior_cnt)                                   AS pv,
        COUNT(DISTINCT user_id)                             AS uv,
        ROUND(SUM(behavior_cnt) / COUNT(DISTINCT user_id), 2) AS avg_behavior,
        dt
    FROM ecommerce_db.dws_user_behavior_1d
    GROUP BY dt
""")

# 3.2 行为类型分布
# SUM(cnt) OVER (PARTITION BY dt) 是窗口函数：
# 在"按 dt 分组"的范围内求总和，即当天所有行为的总次数，用来当分母
spark.sql("""
    INSERT OVERWRITE TABLE ecommerce_db.ads_behavior_distribution PARTITION(dt)
    SELECT
        event_type,
        cnt,
        ROUND(cnt / SUM(cnt) OVER (PARTITION BY dt), 4) AS ratio,
        dt
    FROM (
        SELECT event_type, COUNT(*) AS cnt, dt
        FROM ecommerce_db.dwd_user_behavior
        GROUP BY event_type, dt
    ) t
""")

# 3.3 商品 TOP10：浏览/加购/购买三种排行
# ROW_NUMBER() OVER (PARTITION BY dt ORDER BY xxx_cnt DESC)：
# 每个 dt 分区内按次数从大到小编号 1,2,3...，取编号 <= 10 的就是每天 TOP10
spark.sql("""
    INSERT OVERWRITE TABLE ecommerce_db.ads_product_topn PARTITION(dt)
    SELECT product_id, 'view' AS metric_type, cnt, rank, dt
    FROM (
        SELECT product_id, view_cnt AS cnt,
               ROW_NUMBER() OVER (PARTITION BY dt ORDER BY view_cnt DESC) AS rank,
               dt
        FROM ecommerce_db.dws_product_behavior_1d
    ) t WHERE rank <= 10
    UNION ALL
    SELECT product_id, 'cart' AS metric_type, cnt, rank, dt
    FROM (
        SELECT product_id, cart_cnt AS cnt,
               ROW_NUMBER() OVER (PARTITION BY dt ORDER BY cart_cnt DESC) AS rank,
               dt
        FROM ecommerce_db.dws_product_behavior_1d
    ) t WHERE rank <= 10
    UNION ALL
    SELECT product_id, 'buy' AS metric_type, cnt, rank, dt
    FROM (
        SELECT product_id, buy_cnt AS cnt,
               ROW_NUMBER() OVER (PARTITION BY dt ORDER BY buy_cnt DESC) AS rank,
               dt
        FROM ecommerce_db.dws_product_behavior_1d
    ) t WHERE rank <= 10
""")

# 3.4 品类访问 TOP10
spark.sql("""
    INSERT OVERWRITE TABLE ecommerce_db.ads_category_topn PARTITION(dt)
    SELECT category_id, category_code, view_cnt, rank, dt
    FROM (
        SELECT category_id, category_code, view_cnt,
               ROW_NUMBER() OVER (PARTITION BY dt ORDER BY view_cnt DESC) AS rank,
               dt
        FROM ecommerce_db.dws_category_behavior_1d
    ) t WHERE rank <= 10
""")

# 3.5 转化漏斗（用去重用户数，非行为次数）
# view 环节 = 浏览过的用户数；cart = 加购过的用户数；buy = 购买过的用户数
# rate 统一除以首环节(浏览)的用户数：
#   cart 行 = 加购率，buy 行 = 整体购买转化率
# FIRST_VALUE(user_cnt) OVER (PARTITION BY dt ORDER BY sort_order)：
#   取"分组内排序第一"的值，这里就是 view 环节的用户数，作为分母
spark.sql("""
    INSERT OVERWRITE TABLE ecommerce_db.ads_conversion_funnel PARTITION(dt)
    SELECT
        step,
        user_cnt,
        ROUND(user_cnt / FIRST_VALUE(user_cnt) OVER (PARTITION BY dt ORDER BY sort_order), 4) AS rate,
        dt
    FROM (
        SELECT 'view' AS step, 1 AS sort_order,
               COUNT(DISTINCT CASE WHEN view_cnt > 0 THEN user_id END) AS user_cnt,
               dt
        FROM ecommerce_db.dws_user_behavior_1d
        GROUP BY dt
        UNION ALL
        SELECT 'cart' AS step, 2 AS sort_order,
               COUNT(DISTINCT CASE WHEN cart_cnt > 0 THEN user_id END) AS user_cnt,
               dt
        FROM ecommerce_db.dws_user_behavior_1d
        GROUP BY dt
        UNION ALL
        SELECT 'buy' AS step, 3 AS sort_order,
               COUNT(DISTINCT CASE WHEN buy_cnt > 0 THEN user_id END) AS user_cnt,
               dt
        FROM ecommerce_db.dws_user_behavior_1d
        GROUP BY dt
    ) t
""")

# ---------- 4. 验证：直接看结果 ----------
print("=== 流量指标 ===")
spark.sql("SELECT * FROM ads_traffic_metrics").show()

print("=== 行为类型分布 ===")
spark.sql("SELECT * FROM ads_behavior_distribution").show()

print("=== 商品浏览TOP10 ===")
spark.sql("SELECT * FROM ads_product_topn WHERE metric_type='view' ORDER BY rank").show()

print("=== 品类访问TOP10 ===")
spark.sql("SELECT * FROM ads_category_topn ORDER BY rank").show()

print("=== 转化漏斗 ===")
spark.sql("SELECT * FROM ads_conversion_funnel").show()

spark.stop()
