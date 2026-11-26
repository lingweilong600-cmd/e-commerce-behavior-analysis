# -*- coding: utf-8 -*-
# 第四阶段(2)：ODS 表 -> DWD 表（转类型 + 清洗）

import os
os.environ["HADOOP_USER_NAME"] = "root"  # 解决 HDFS 权限

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 创建 SparkSession（连接配置）
spark = SparkSession.builder \
    .appName("load_dwd") \
    .master("local[*]") \
    .config("spark.sql.warehouse.dir", "hdfs://node1:8020/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://node1:9083") \
    .config("hive.exec.dynamic.partition", "true") \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("USE ecommerce_db")

# 建 DWD 表（IF NOT EXISTS，重复执行不报错）
spark.sql("""
    CREATE TABLE IF NOT EXISTS dwd_user_behavior (
        event_time     TIMESTAMP COMMENT '事件时间(已解析)',
        event_type     STRING    COMMENT '行为类型: view/cart/purchase',
        product_id     BIGINT    COMMENT '商品ID',
        category_id    BIGINT    COMMENT '品类ID',
        category_code  STRING    COMMENT '品类代码',
        brand          STRING    COMMENT '品牌',
        price          DOUBLE    COMMENT '价格',
        user_id        BIGINT    COMMENT '用户ID',
        user_session   STRING    COMMENT '会话ID'
    )
    COMMENT 'DWD 用户行为明细表(已清洗)'
    PARTITIONED BY (dt STRING)
    STORED AS PARQUET
""")

# 从 ODS 读回，开始清洗
dwd = spark.table("ods_user_behavior")

# 1) 字符串 -> 真实类型（非法值会自动变 null）
dwd = dwd.withColumn("product_id",  F.col("product_id").cast("bigint"))
dwd = dwd.withColumn("category_id", F.col("category_id").cast("bigint"))
dwd = dwd.withColumn("price",       F.col("price").cast("double"))
dwd = dwd.withColumn("user_id",     F.col("user_id").cast("bigint"))

# 2) 时间解析：去掉 " UTC" 后缀再转时间戳
dwd = dwd.withColumn(
    "event_time",
    F.to_timestamp(
        F.regexp_replace(F.trim(F.col("event_time")), " UTC$", ""),
        "yyyy-MM-dd HH:mm:ss"
    )
)

# 3) 整行去重
dwd = dwd.dropDuplicates()

# 4) 只保留合法行为类型（本数据集只有 view / cart / purchase）
dwd = dwd.filter(F.col("event_type").isin("view", "cart", "purchase"))

# 5) 关键 ID 不能为空（非法值转类型后已变 null）
dwd = dwd.filter(
    F.col("product_id").isNotNull() &
    F.col("category_id").isNotNull() &
    F.col("user_id").isNotNull()
)

# 6) 价格必须大于 0
dwd = dwd.filter(F.col("price") > 0)

# 7) 缺失值处理：category_code / brand 补 "unknown"，user_session 空串转 null
dwd = dwd.replace("", "unknown", subset=["category_code", "brand"]) \
         .fillna("unknown", subset=["category_code", "brand"])

dwd = dwd.withColumn(
    "user_session",
    F.when(F.col("user_session") == "", F.lit(None)).otherwise(F.col("user_session"))
)

# dt 分区列 ODS 已经带过来，直接沿用

# 写 DWD 表（动态分区）
dwd.write.mode("overwrite").insertInto("dwd_user_behavior")
print("DWD 表写入完成")

# 验证：总行数 + 行为分布（view 482053 / purchase 9595 / cart 7553 ）
spark.sql("SELECT count(*) AS cnt FROM dwd_user_behavior").show()
spark.sql("""
    SELECT event_type, count(*) AS cnt
    FROM dwd_user_behavior
    GROUP BY event_type
    ORDER BY cnt DESC
""").show()

spark.stop()
