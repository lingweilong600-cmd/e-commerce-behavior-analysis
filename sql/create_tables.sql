-- ============================================================
-- 电商用户行为分析数仓 - 建库建表脚本
-- 数据源 : REES46 多品类电商行为数据 (2019-Nov.csv)
-- 分层   : ODS -> DWD -> DWS -> ADS
-- 存储   : PARQUET（列式存储，压缩比高、查询快）
-- 分区   : 所有表统一按业务日期 dt (yyyy-MM-dd) 分区
-- ============================================================

-- 1. 创建数据库
CREATE DATABASE IF NOT EXISTS ecommerce_db
COMMENT '电商用户行为分析数仓';

USE ecommerce_db;


-- ============================================================
-- 2. ODS 原始层（贴源层）
--    原则：原样落地，不做任何加工，字段全部 STRING 保留原始内容，
--    保证数据可回溯、可重算。
-- ============================================================
CREATE TABLE IF NOT EXISTS ecommerce_db.ods_user_behavior (
    event_time      STRING COMMENT '事件时间(原始字符串，如 2019-11-01 00:00:00 UTC)',
    event_type      STRING COMMENT '行为类型: view / cart / purchase',
    product_id      STRING COMMENT '商品ID',
    category_id     STRING COMMENT '品类ID',
    category_code   STRING COMMENT '品类代码',
    brand           STRING COMMENT '品牌',
    price           STRING COMMENT '价格',
    user_id         STRING COMMENT '用户ID',
    user_session    STRING COMMENT '会话ID'
)
COMMENT '用户行为原始数据表(贴源层)'
PARTITIONED BY (dt STRING)
STORED AS PARQUET;


-- ============================================================
-- 3. DWD 明细层（清洗层）
--    在 ODS 基础上完成：去重、缺失值处理、类型转换、时间解析。
--    类型恢复为真实业务类型，异常值已在清洗阶段剔除。
-- ============================================================
CREATE TABLE IF NOT EXISTS ecommerce_db.dwd_user_behavior (
    event_time      TIMESTAMP COMMENT '事件时间(已解析为时间戳)',
    event_type      STRING    COMMENT '行为类型: view / cart / purchase',
    product_id      BIGINT    COMMENT '商品ID',
    category_id     BIGINT    COMMENT '品类ID(数值很大，必须 BIGINT)',
    category_code   STRING    COMMENT '品类代码(缺失补 unknown)',
    brand           STRING    COMMENT '品牌(缺失补 unknown)',
    price           DOUBLE    COMMENT '价格(已过滤 <=0 的异常值)',
    user_id         BIGINT    COMMENT '用户ID',
    user_session    STRING    COMMENT '会话ID'
)
COMMENT '用户行为明细表(清洗层)'
PARTITIONED BY (dt STRING)
STORED AS PARQUET;


-- ============================================================
-- 4. DWS 汇总层（按主题轻度聚合，三个主题各一张表）
--    粒度：每个 dt 分区内，一个主体聚合为一行。
-- ============================================================

-- 4.1 用户主题：每日每用户的行为次数
CREATE TABLE IF NOT EXISTS ecommerce_db.dws_user_behavior_1d (
    user_id         BIGINT COMMENT '用户ID',
    view_cnt        BIGINT COMMENT '浏览次数',
    cart_cnt        BIGINT COMMENT '加购次数',
    buy_cnt         BIGINT COMMENT '购买次数',
    behavior_cnt    BIGINT COMMENT '总行为次数'
)
COMMENT '用户行为汇总表(按天-用户粒度)'
PARTITIONED BY (dt STRING)
STORED AS PARQUET;

-- 4.2 商品主题：每日每商品的行为次数
CREATE TABLE IF NOT EXISTS ecommerce_db.dws_product_behavior_1d (
    product_id      BIGINT COMMENT '商品ID',
    view_cnt        BIGINT COMMENT '被浏览次数',
    cart_cnt        BIGINT COMMENT '被加购次数',
    buy_cnt         BIGINT COMMENT '被购买次数',
    behavior_cnt    BIGINT COMMENT '总行为次数'
)
COMMENT '商品行为汇总表(按天-商品粒度)'
PARTITIONED BY (dt STRING)
STORED AS PARQUET;

-- 4.3 品类主题：每日每品类的行为次数
CREATE TABLE IF NOT EXISTS ecommerce_db.dws_category_behavior_1d (
    category_id     BIGINT COMMENT '品类ID',
    category_code   STRING COMMENT '品类代码',
    view_cnt        BIGINT COMMENT '访问次数',
    cart_cnt        BIGINT COMMENT '加购次数',
    buy_cnt         BIGINT COMMENT '购买次数',
    behavior_cnt    BIGINT COMMENT '总行为次数'
)
COMMENT '品类行为汇总表(按天-品类粒度)'
PARTITIONED BY (dt STRING)
STORED AS PARQUET;


-- ============================================================
-- 5. ADS 应用层（最终指标结果表，可直接对外展示）
-- ============================================================

-- 5.1 流量指标：PV / UV(即当天日活DAU) / 人均行为次数
CREATE TABLE IF NOT EXISTS ecommerce_db.ads_traffic_metrics (
    pv            BIGINT COMMENT 'PV(总记录数，不去重)',
    uv            BIGINT COMMENT 'UV(去重用户数)',
    avg_behavior  DOUBLE COMMENT '人均行为次数 = pv / uv'
)
COMMENT '流量指标结果表'
PARTITIONED BY (dt STRING)
STORED AS PARQUET;

-- 5.2 行为类型分布
CREATE TABLE IF NOT EXISTS ecommerce_db.ads_behavior_distribution (
    event_type    STRING COMMENT '行为类型',
    cnt           BIGINT COMMENT '记录数',
    ratio         DOUBLE COMMENT '占比'
)
COMMENT '行为类型分布结果表'
PARTITIONED BY (dt STRING)
STORED AS PARQUET;

-- 5.3 商品排行 TOP10（metric_type 区分浏览/加购/购买三种排行）
CREATE TABLE IF NOT EXISTS ecommerce_db.ads_product_topn (
    product_id    BIGINT COMMENT '商品ID',
    metric_type   STRING COMMENT '指标类型: view / cart / buy',
    cnt           BIGINT COMMENT '次数',
    rank          INT    COMMENT '排名(1-10)'
)
COMMENT '商品排行TOP10结果表'
PARTITIONED BY (dt STRING)
STORED AS PARQUET;

-- 5.4 品类访问排行
CREATE TABLE IF NOT EXISTS ecommerce_db.ads_category_topn (
    category_id   BIGINT COMMENT '品类ID',
    category_code STRING COMMENT '品类代码',
    view_cnt      BIGINT COMMENT '访问次数',
    rank          INT    COMMENT '排名'
)
COMMENT '品类访问排行结果表'
PARTITIONED BY (dt STRING)
STORED AS PARQUET;

-- 5.5 转化漏斗：浏览 -> 加购 -> 购买（去重用户数 + 转化率）
CREATE TABLE IF NOT EXISTS ecommerce_db.ads_conversion_funnel (
    step          STRING COMMENT '漏斗环节: view / cart / buy',
    user_cnt      BIGINT COMMENT '该环节去重用户数',
    rate          DOUBLE COMMENT '相对首环节(浏览)的转化率，小数形式，如 0.08 表示 8%'
)
COMMENT '转化漏斗结果表'
PARTITIONED BY (dt STRING)
STORED AS PARQUET;
