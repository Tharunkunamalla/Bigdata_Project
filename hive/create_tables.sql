-- ============================================================================
-- CSE412 Big Data Project: Hive Schema Definitions
-- Database: cse412_disaster
-- ============================================================================

CREATE DATABASE IF NOT EXISTS cse412_disaster
COMMENT 'Database for real-time crisis misinformation detection and emergency resource allocation'
LOCATION '/disaster_data/hive_warehouse';

USE cse412_disaster;

-- ----------------------------------------------------------------------------
-- 1. Silver Layer: Partitioned External Table for Cleaned Crisis Posts
-- ----------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS disaster_posts_silver (
    post_id STRING,
    event_name STRING,
    timestamp_utc TIMESTAMP,
    created_date DATE,
    text STRING,
    user_id STRING,
    user_followers_count INT,
    user_account_age_days INT,
    user_verified BOOLEAN,
    retweet_count INT,
    has_external_link BOOLEAN,
    latitude DOUBLE,
    longitude DOUBLE,
    location_name STRING,
    humanitarian_category STRING,
    urgency_level DOUBLE,
    is_synthetic INT,
    is_rumour INT,
    source_provenance STRING,
    char_length INT,
    word_count INT,
    avg_word_length DOUBLE,
    uppercase_ratio DOUBLE,
    exclamation_count INT,
    burstiness_score DOUBLE,
    lexical_diversity DOUBLE,
    synthetic_confidence_prior DOUBLE,
    credibility_raw_score DOUBLE
)
COMMENT 'Curated disaster social reports from silver layer'
PARTITIONED BY (event_type STRING)
STORED AS PARQUET
LOCATION '/disaster_data/silver/disaster_posts';

-- Recover partitions after writing parquet files
MSCK REPAIR TABLE disaster_posts_silver;

-- ----------------------------------------------------------------------------
-- 2. Gold Layer: Aggregated Emergency Demand by Event and Region
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold_emergency_demand_summary (
    event_type STRING,
    location_name STRING,
    total_reports BIGINT,
    high_urgency_reports BIGINT,
    synthetic_misinformation_count BIGINT,
    misinformation_ratio DOUBLE,
    avg_urgency DOUBLE,
    computed_at TIMESTAMP
)
COMMENT 'Gold analytical summary for downstream emergency resource allocation'
STORED AS PARQUET;

-- ----------------------------------------------------------------------------
-- 3. Gold Layer: Trust and Credibility Metrics by User Cluster
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold_trust_metrics (
    source_provenance STRING,
    is_synthetic INT,
    total_authors BIGINT,
    avg_followers DOUBLE,
    avg_burstiness DOUBLE,
    avg_synthetic_confidence DOUBLE
)
STORED AS PARQUET;
