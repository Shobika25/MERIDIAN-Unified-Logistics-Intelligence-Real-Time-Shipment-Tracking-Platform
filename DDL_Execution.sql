
-- =====================================================
-- PROJECT MERIDIAN
-- LOGISTICS DATA MART - GOLD LAYER
-- STAR SCHEMA DDL
-- Total: 4 Dimension Tables + 2 Fact Tables
-- Storage Format: Apache Iceberg
-- Catalog: meridian
-- =====================================================


-- STEP 1: CREATE GOLD NAMESPACE

CREATE NAMESPACE IF NOT EXISTS meridian.gold;


-- =====================================================
-- STEP 2: CREATE DIMENSION TABLES
-- =====================================================

-- 1. DIM_CUSTOMER
-- Grain: One row per unique customer

CREATE TABLE IF NOT EXISTS meridian.gold.dim_customer (
    customer_key BIGINT,
    customer_id STRING,
    customer_name STRING,
    email STRING,
    city STRING,
    segment STRING,
    signup_date DATE
)
USING ICEBERG;


-- 2. DIM_CARRIER
-- Grain: One row per unique carrier

CREATE TABLE IF NOT EXISTS meridian.gold.dim_carrier (
    carrier_key BIGINT,
    carrier_id STRING,
    carrier_name STRING,
    transport_mode STRING,
    service_level STRING,
    rate_per_kg_km_inr DOUBLE,
    sla_on_time_pct DOUBLE,
    active BOOLEAN
)
USING ICEBERG;


-- 3. DIM_HUB
-- Grain: One row per unique logistics hub

CREATE TABLE IF NOT EXISTS meridian.gold.dim_hub (
    hub_key BIGINT,
    hub_id STRING,
    hub_name STRING,
    city STRING,
    state STRING,
    region STRING,
    hub_type STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    capacity_shipments_per_day INT,
    active BOOLEAN
)
USING ICEBERG;


-- 4. DIM_DATE
-- Grain: One row per calendar date

CREATE TABLE IF NOT EXISTS meridian.gold.dim_date (
    date_key INT,
    full_date DATE,
    day INT,
    month INT,
    month_name STRING,
    quarter INT,
    year INT,
    day_of_week STRING,
    is_weekend BOOLEAN
)
USING ICEBERG;


-- =====================================================
-- STEP 3: CREATE FACT TABLES
-- =====================================================

-- 5. FACT_SHIPMENT
-- Grain: One row per unique shipment
-- Stores shipment details, measures and delivery performance

CREATE TABLE IF NOT EXISTS meridian.gold.fact_shipment (
    shipment_id STRING,
    customer_key BIGINT,
    carrier_key BIGINT,
    origin_hub_key BIGINT,
    destination_hub_key BIGINT,
    ship_date_key INT,
    promised_date_key INT,
    delivery_date_key INT,
    weight_kg DOUBLE,
    distance_km DOUBLE,
    freight_charge_inr DOUBLE,
    shipment_status STRING,
    delay_reason STRING,
    transit_days INT,
    is_on_time BOOLEAN
)
USING ICEBERG;


-- 6. FACT_TRACKING_EVENT
-- Grain: One row per unique tracking event
-- Stores shipment tracking activity and delay information

CREATE TABLE IF NOT EXISTS meridian.gold.fact_tracking_event (
    event_id STRING,
    shipment_id STRING,
    hub_key BIGINT,
    event_date_key INT,
    event_type STRING,
    event_time TIMESTAMP,
    ingest_time TIMESTAMP,
    vehicle_id STRING,
    delay_minutes INT,
    event_count INT
)
USING ICEBERG;


-- =====================================================
-- STEP 4: VERIFY TABLE CREATION
-- =====================================================

SHOW TABLES IN meridian.gold;

-- =====================================================
-- END OF LOGISTICS DATA MART DDL
-- =====================================================
