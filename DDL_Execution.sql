
-- =========================================================
-- PROJECT MERIDIAN
-- LOGISTICS DATA MART - SINGLE STAR SCHEMA
-- Storage Format: Apache Iceberg
-- Catalog: meridian
--
-- Total Tables: 5
-- Dimension Tables: 4
-- Fact Tables: 1
-- =========================================================


-- STEP 1: CREATE GOLD NAMESPACE

CREATE NAMESPACE IF NOT EXISTS meridian.gold;


-- =========================================================
-- STEP 2: CREATE DIMENSION TABLES
-- =========================================================


-- 1. DIM_CUSTOMER
-- Grain: One row per unique customer
-- Primary Key: customer_key

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
-- Primary Key: carrier_key

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
-- Primary Key: hub_key

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
-- Primary Key: date_key

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


-- =========================================================
-- STEP 3: CREATE CENTRAL FACT TABLE
-- =========================================================


-- 5. FACT_SHIPMENT
--
-- Grain: One row per unique shipment
--
-- Primary Key: shipment_id
--
-- Foreign Keys:
-- customer_key       -> DIM_CUSTOMER.customer_key
-- carrier_key        -> DIM_CARRIER.carrier_key
-- origin_hub_key     -> DIM_HUB.hub_key
-- destination_hub_key -> DIM_HUB.hub_key
-- ship_date_key      -> DIM_DATE.date_key
-- promised_date_key  -> DIM_DATE.date_key
-- delivery_date_key  -> DIM_DATE.date_key
--
-- Contains shipment measures and aggregated
-- tracking event measures.


CREATE TABLE IF NOT EXISTS meridian.gold.fact_shipment (

    -- Primary Key
    shipment_id STRING,

    -- Foreign Keys
    customer_key BIGINT,
    carrier_key BIGINT,
    origin_hub_key BIGINT,
    destination_hub_key BIGINT,

    ship_date_key INT,
    promised_date_key INT,
    delivery_date_key INT,

    -- Shipment Attributes
    shipment_status STRING,
    delay_reason STRING,

    -- Shipment Measures
    weight_kg DOUBLE,
    distance_km DOUBLE,
    freight_charge_inr DOUBLE,
    transit_days INT,
    is_on_time BOOLEAN,

    -- Aggregated Tracking Measures
    total_delay_minutes BIGINT,
    tracking_event_count INT,
    hub_stops_count INT,
    last_event_time TIMESTAMP

)
USING ICEBERG;


-- =========================================================
-- STEP 4: VERIFY TABLE CREATION
-- =========================================================

SHOW TABLES IN meridian.gold;


-- =========================================================
-- STEP 5: VERIFY TABLE STRUCTURES
-- =========================================================

DESCRIBE TABLE meridian.gold.dim_customer;

DESCRIBE TABLE meridian.gold.dim_carrier;

DESCRIBE TABLE meridian.gold.dim_hub;

DESCRIBE TABLE meridian.gold.dim_date;

DESCRIBE TABLE meridian.gold.fact_shipment;


-- =========================================================
-- END OF PROJECT MERIDIAN STAR SCHEMA DDL
-- =========================================================
