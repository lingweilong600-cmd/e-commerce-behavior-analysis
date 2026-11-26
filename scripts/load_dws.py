# -*- coding: utf-8 -*-
# 第五阶段(1)：DWD -> DWS 汇总层
# 从清洗后的明细表，按天汇总出三张表（"每个主体每天一行"）：
#   dws_user_behavior_1d     每个用户每天的行为次数
#   dws_product_behavior_1d  每个商品每天被浏览/加购/购买的次数
#   dws_category_behavior_1d 每个品类每天被浏览/加购/购买的次数
# 后续 ADS 指标层直接从这三张表取数。

import os
os.environ["HADOOP_USER_NAME"] = "root"  # 解决 HDFS 权限

from pyspark.sql import SparkSession

# ---------- 1. 创建 SparkSession，开启 Hive ----------
spark = SparkSession.builder \
    .appName("load_dws") \
    .master("local[*]") \
    .config("spark.sql.warehouse.dir", "hdfs://node1:8020/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://node1:9083") \
    .config("hive.exec.dynamic.partition", "true") \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("USE ecommerce_db")

# ---------- 2. 建三张 DWS 表（IF NOT EXISTS，重复执行不报错） ----------
spark.sql("""
    CREATE TABLE IF NOT EXISTS dws_user_behavior_1d (
        user_id       BIGINT COMMENT '用户ID',
        view_cnt      BIGINT COMMENT '浏览次数',
        cart_cnt      BIGINT COMMENT '加购次数',
        buy_cnt       BIGINT COMMENT '购买次数',
        behavior_cnt  BIGINT COMMENT '总行为次数'
    )
    COMMENT '用户行为汇总表(按天-用户粒度)'
    PARTITIONED BY (dt STRING)
    STORED AS PARQUET
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS dws_product_behavior_1d (
        product_id    BIGINT COMMENT '商品ID',
        view_cnt      BIGINT COMMENT '被浏览次数',
        cart_cnt      BIGINT COMMENT '被加购次数',
        buy_cnt       BIGINT COMMENT '被购买次数',
        behavior_cnt  BIGINT COMMENT '总行为次数'
    )
    COMMENT '商品行为汇总表(按天-商品粒度)'
    PARTITIONED BY (dt STRING)
    STORED AS PARQUET
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS dws_category_behavior_1d (
        category_id   BIGINT COMMENT '品类ID',
        category_code STRING COMMENT '品类代码',
        view_cnt      BIGINT COMMENT '访问次数',
        cart_cnt      BIGINT COMMENT '加购次数',
        buy_cnt       BIGINT COMMENT '购买次数',
        behavior_cnt  BIGINT COMMENT '总行为次数'
    )
    COMMENT '品类行为汇总表(按天-品类粒度)'
    PARTITIONED BY (dt STRING)
    STORED AS PARQUET
""")

# ---------- 3. 写数据（INSERT OVERWRITE 动态分区：重跑只覆盖写入的那几天） ----------

# 3.1 用户粒度：CASE WHEN 把一行行为记录变成"是这种行为的记 1，否则记 0"，再求和
spark.sql("""
    INSERT OVERWRITE TABLE ecommerce_db.dws_user_behavior_1d PARTITION(dt)
    SELECT
        user_id,
        SUM(CASE WHEN event_type = 'view'     THEN 1 ELSE 0 END) AS view_cnt,
        SUM(CASE WHEN event_type = 'cart'     THEN 1 ELSE 0 END) AS cart_cnt,
        SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS buy_cnt,
        COUNT(*) AS behavior_cnt,
        dt
    FROM ecommerce_db.dwd_user_behavior
    GROUP BY user_id, dt
""")

# 3.2 商品粒度
spark.sql("""
    INSERT OVERWRITE TABLE ecommerce_db.dws_product_behavior_1d PARTITION(dt)
    SELECT
        product_id,
        SUM(CASE WHEN event_type = 'view'     THEN 1 ELSE 0 END) AS view_cnt,
        SUM(CASE WHEN event_type = 'cart'     THEN 1 ELSE 0 END) AS cart_cnt,
        SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS buy_cnt,
        COUNT(*) AS behavior_cnt,
        dt
    FROM ecommerce_db.dwd_user_behavior
    GROUP BY product_id, dt
""")

# 3.3 品类粒度（category_code 和 category_id 一一对应，用 MAX 取一个即可）
spark.sql("""
    INSERT OVERWRITE TABLE ecommerce_db.dws_category_behavior_1d PARTITION(dt)
    SELECT
        category_id,
        MAX(category_code) AS category_code,
        SUM(CASE WHEN event_type = 'view'     THEN 1 ELSE 0 END) AS view_cnt,
        SUM(CASE WHEN event_type = 'cart'     THEN 1 ELSE 0 END) AS cart_cnt,
        SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS buy_cnt,
        COUNT(*) AS behavior_cnt,
        dt
    FROM ecommerce_db.dwd_user_behavior
    GROUP BY category_id, dt
""")

# ---------- 4. 验证 ----------
print("=== 各表记录数 ===")
spark.sql("SELECT count(*) AS user_cnt FROM dws_user_behavior_1d").show()
spark.sql("SELECT count(*) AS product_cnt FROM dws_product_behavior_1d").show()
spark.sql("SELECT count(*) AS category_cnt FROM dws_category_behavior_1d").show()

print("=== 用户汇总表抽样 ===")
spark.sql("SELECT * FROM dws_user_behavior_1d LIMIT 5").show()

spark.stop()
