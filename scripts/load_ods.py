# -*- coding: utf-8 -*-
# 第四阶段(1)：原始 CSV -> ODS 表（原样落地）
# ODS 原则：不加工、不转类型，全部按字符串原样保存，只加一个分区列 dt

import os
os.environ["HADOOP_USER_NAME"] = "root"  # 解决 HDFS 权限

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 创建 SparkSession（连接配置spark on hive）
spark = SparkSession.builder \
    .appName("load_ods") \
    .master("local[*]") \
    .config("spark.sql.warehouse.dir", "hdfs://node1:8020/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://node1:9083") \
    .config("hive.exec.dynamic.partition", "true") \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .enableHiveSupport() \
    .getOrCreate()

# 建库 + 建 ODS 表（IF NOT EXISTS，重复执行不报错）
spark.sql("CREATE DATABASE IF NOT EXISTS ecommerce_db")
spark.sql("USE ecommerce_db")

spark.sql("""
    CREATE TABLE IF NOT EXISTS ods_user_behavior (
        event_time     STRING COMMENT '事件时间(原始字符串)',
        event_type     STRING COMMENT '行为类型: view/cart/purchase',
        product_id     STRING COMMENT '商品ID',
        category_id    STRING COMMENT '品类ID',
        category_code  STRING COMMENT '品类代码',
        brand          STRING COMMENT '品牌',
        price          STRING COMMENT '价格',
        user_id        STRING COMMENT '用户ID',
        user_session   STRING COMMENT '会话ID'
    )
    COMMENT 'ODS 用户行为原始数据表'
    PARTITIONED BY (dt STRING)
    STORED AS PARQUET
""")

# 读原始 CSV：不指定 schema，全部读成字符串，保证原样落地
raw = spark.read \
    .option("header", "true") \
    .csv("hdfs://node1:8020/data/sample_500k.csv")

print("原始记录数:", raw.count())

# 只加分区列 dt（取 event_time 的日期部分），其余字段不动
ods_df = raw.withColumn("dt", F.substring_index(F.col("event_time"), " ", 1))

# 写 ODS 表（动态分区，自动生成 dt=2019-11-xx 目录）
ods_df.write.mode("overwrite").insertInto("ods_user_behavior")
print("ODS 表写入完成")

# 验证：总行数 + 每天的数据量（确认分区正确）
spark.sql("SELECT count(*) AS cnt FROM ods_user_behavior").show()
spark.sql("SELECT dt, count(*) AS cnt FROM ods_user_behavior GROUP BY dt ORDER BY dt").show()

spark.stop()
