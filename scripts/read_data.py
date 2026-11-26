# -*- coding: utf-8 -*-
# 数据读取模块
import os
os.environ["HADOOP_USER_NAME"] = "root"  # 解决 HDFS 权限

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

if __name__ == '__main__':
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("read_data") \
        .getOrCreate()

    schema = StructType([
        StructField("event_time",    StringType(), True),
        StructField("event_type",    StringType(), True),
        StructField("product_id",    LongType(),   True),
        StructField("category_id",   LongType(),   True),
        StructField("category_code", StringType(), True),
        StructField("brand",         StringType(), True),
        StructField("price",         DoubleType(), True),
        StructField("user_id",       LongType(),   True),
        StructField("user_session",  StringType(), True),
    ])

    df = spark.read \
        .option("header", "true") \
        .schema(schema) \
        .csv("hdfs://node1:8020/data/sample_500k.csv")

    df.printSchema()
    df.show(5, truncate=False)

    df.groupBy("event_type").count().show()
