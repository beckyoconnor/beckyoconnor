/*-----------------------------------------------------------------------------
Hands-On Lab: Data Engineering with Snowpark
Script:       02_load_raw.sql
Author:       Jeremiah Hansen
Last Updated: 1/1/2023
-----------------------------------------------------------------------------*/

USE ROLE HOL_ROLE;
USE WAREHOUSE HOL_WH;
USE DATABASE HOL_DB;


-- ----------------------------------------------------------------------------
-- Step #1: Load raw Frostbyte POS data from Parquet files
-- ----------------------------------------------------------------------------
USE SCHEMA RAW_POS;


-- Country
-- Debug: LIST @external.frostbyte_raw_stage/tko/pos/country;
-- Debug: SELECT * FROM @external.frostbyte_raw_stage/tko/pos/country/country.snappy.parquet (FILE_FORMAT => EXTERNAL.PARQUET_FORMAT) LIMIT 100;

CREATE OR REPLACE TABLE COUNTRY
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(
      INFER_SCHEMA(
        LOCATION=>'@external.frostbyte_raw_stage/tko/pos/country',
        FILE_FORMAT=>'EXTERNAL.PARQUET_FORMAT'
      )
    )
  );

COPY INTO COUNTRY
FROM @external.frostbyte_raw_stage/tko/pos/country
FILE_FORMAT = (FORMAT_NAME = EXTERNAL.PARQUET_FORMAT)
MATCH_BY_COLUMN_NAME = CASE_SENSITIVE;

-- Debug: SELECT * FROM COUNTRY LIMIT 100;


-- Franchise
-- Debug: LIST @external.frostbyte_raw_stage/tko/pos/franchise;
-- Debug: SELECT * FROM @external.frostbyte_raw_stage/tko/pos/franchise/franchise.snappy.parquet (FILE_FORMAT => EXTERNAL.PARQUET_FORMAT) LIMIT 100;

CREATE OR REPLACE TABLE FRANCHISE
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(
      INFER_SCHEMA(
        LOCATION=>'@external.frostbyte_raw_stage/tko/pos/franchise',
        FILE_FORMAT=>'EXTERNAL.PARQUET_FORMAT'
      )
    )
  );

COPY INTO FRANCHISE
FROM @external.frostbyte_raw_stage/tko/pos/franchise
FILE_FORMAT = (FORMAT_NAME = EXTERNAL.PARQUET_FORMAT)
MATCH_BY_COLUMN_NAME = CASE_SENSITIVE;

-- Debug: SELECT * FROM FRANCHISE LIMIT 100;


-- Location
-- Debug: LIST @external.frostbyte_raw_stage/tko/pos/location;
-- Debug: SELECT * FROM @external.frostbyte_raw_stage/tko/pos/location/location.snappy.parquet (FILE_FORMAT => EXTERNAL.PARQUET_FORMAT) LIMIT 100;

CREATE OR REPLACE TABLE LOCATION
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(
      INFER_SCHEMA(
        LOCATION=>'@external.frostbyte_raw_stage/tko/pos/location',
        FILE_FORMAT=>'EXTERNAL.PARQUET_FORMAT'
      )
    )
  );

COPY INTO LOCATION
FROM @external.frostbyte_raw_stage/tko/pos/location
FILE_FORMAT = (FORMAT_NAME = EXTERNAL.PARQUET_FORMAT)
MATCH_BY_COLUMN_NAME = CASE_SENSITIVE;

-- Debug: SELECT * FROM LOCATION LIMIT 100;


-- Menu
-- Debug: LIST @external.frostbyte_raw_stage/tko/pos/menu;
-- Debug: SELECT * FROM @external.frostbyte_raw_stage/tko/pos/menu/menu.snappy.parquet (FILE_FORMAT => EXTERNAL.PARQUET_FORMAT) LIMIT 100;

CREATE OR REPLACE TABLE MENU
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(
      INFER_SCHEMA(
        LOCATION=>'@external.frostbyte_raw_stage/tko/pos/menu',
        FILE_FORMAT=>'EXTERNAL.PARQUET_FORMAT'
      )
    )
  );

COPY INTO MENU
FROM @external.frostbyte_raw_stage/tko/pos/menu
FILE_FORMAT = (FORMAT_NAME = EXTERNAL.PARQUET_FORMAT)
MATCH_BY_COLUMN_NAME = CASE_SENSITIVE;

-- Debug: SELECT * FROM MENU LIMIT 100;


-- Truck
-- Debug: LIST @external.frostbyte_raw_stage/tko/pos/truck;
-- Debug: SELECT * FROM @external.frostbyte_raw_stage/tko/pos/truck/truck.snappy.parquet (FILE_FORMAT => EXTERNAL.PARQUET_FORMAT) LIMIT 100;

CREATE OR REPLACE TABLE TRUCK
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(
      INFER_SCHEMA(
        LOCATION=>'@external.frostbyte_raw_stage/tko/pos/truck',
        FILE_FORMAT=>'EXTERNAL.PARQUET_FORMAT'
      )
    )
  );

COPY INTO TRUCK
FROM @external.frostbyte_raw_stage/tko/pos/truck
FILE_FORMAT = (FORMAT_NAME = EXTERNAL.PARQUET_FORMAT)
MATCH_BY_COLUMN_NAME = CASE_SENSITIVE;

-- Debug: SELECT * FROM TRUCK LIMIT 100;


ALTER WAREHOUSE HOL_WH SET WAREHOUSE_SIZE = XLARGE;

-- Order Header
-- Debug: LIST @external.frostbyte_raw_stage/tko/pos/order_header;
-- Debug: SELECT * FROM @external.frostbyte_raw_stage/tko/pos/order_header/year=2019/data_01a91b48-0605-6a9c-0000-711101079122_007_0_0.snappy.parquet (FILE_FORMAT => EXTERNAL.PARQUET_FORMAT) LIMIT 100;

CREATE OR REPLACE TABLE ORDER_HEADER
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(
      INFER_SCHEMA(
        LOCATION=>'@external.frostbyte_raw_stage/tko/pos/order_header',
        FILE_FORMAT=>'EXTERNAL.PARQUET_FORMAT'
      )
    )
  );

COPY INTO ORDER_HEADER
FROM @external.frostbyte_raw_stage/tko/pos/order_header/year=2019
FILE_FORMAT = (FORMAT_NAME = EXTERNAL.PARQUET_FORMAT)
MATCH_BY_COLUMN_NAME = CASE_SENSITIVE;

COPY INTO ORDER_HEADER
FROM @external.frostbyte_raw_stage/tko/pos/order_header/year=2020
FILE_FORMAT = (FORMAT_NAME = EXTERNAL.PARQUET_FORMAT)
MATCH_BY_COLUMN_NAME = CASE_SENSITIVE;

COPY INTO ORDER_HEADER
FROM @external.frostbyte_raw_stage/tko/pos/order_header/year=2021
FILE_FORMAT = (FORMAT_NAME = EXTERNAL.PARQUET_FORMAT)
MATCH_BY_COLUMN_NAME = CASE_SENSITIVE;

--COPY INTO ORDER_HEADER
--FROM @external.frostbyte_raw_stage/tko/pos/order_header/year=2022
--FILE_FORMAT = (FORMAT_NAME = EXTERNAL.PARQUET_FORMAT)
--MATCH_BY_COLUMN_NAME = CASE_SENSITIVE;

-- Debug: SELECT * FROM ORDER_HEADER LIMIT 100;


-- Order Detail
-- Debug: LIST @external.frostbyte_raw_stage/tko/pos/order_detail;
-- Debug: SELECT * FROM @external.frostbyte_raw_stage/tko/pos/order_detail/year=2019/data_01a91b50-0605-721e-0000-71110107a166_011_0_0.snappy.parquet (FILE_FORMAT => EXTERNAL.PARQUET_FORMAT) LIMIT 100;

CREATE OR REPLACE TABLE ORDER_DETAIL
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(
      INFER_SCHEMA(
        LOCATION=>'@external.frostbyte_raw_stage/tko/pos/order_detail',
        FILE_FORMAT=>'EXTERNAL.PARQUET_FORMAT'
      )
    )
  );

COPY INTO ORDER_DETAIL
FROM @external.frostbyte_raw_stage/tko/pos/order_detail/year=2019
FILE_FORMAT = (FORMAT_NAME = EXTERNAL.PARQUET_FORMAT)
MATCH_BY_COLUMN_NAME = CASE_SENSITIVE;

COPY INTO ORDER_DETAIL
FROM @external.frostbyte_raw_stage/tko/pos/order_detail/year=2020
FILE_FORMAT = (FORMAT_NAME = EXTERNAL.PARQUET_FORMAT)
MATCH_BY_COLUMN_NAME = CASE_SENSITIVE;

COPY INTO ORDER_DETAIL
FROM @external.frostbyte_raw_stage/tko/pos/order_detail/year=2021
FILE_FORMAT = (FORMAT_NAME = EXTERNAL.PARQUET_FORMAT)
MATCH_BY_COLUMN_NAME = CASE_SENSITIVE;

--COPY INTO ORDER_DETAIL
--FROM @external.frostbyte_raw_stage/tko/pos/order_detail/year=2022
--FILE_FORMAT = (FORMAT_NAME = EXTERNAL.PARQUET_FORMAT)
--MATCH_BY_COLUMN_NAME = CASE_SENSITIVE;

-- Debug: SELECT * FROM ORDER_DETAIL LIMIT 100;


ALTER WAREHOUSE HOL_WH SET WAREHOUSE_SIZE = XSMALL;


-- ----------------------------------------------------------------------------
-- Step #2: Load raw Frostbyte Customer data from Parquet files
-- ----------------------------------------------------------------------------
USE SCHEMA RAW_CUSTOMER;

-- Customer Loyalty
-- Debug: LIST @external.frostbyte_raw_stage/tko/customer;
-- Debug: SELECT * FROM @external.frostbyte_raw_stage/tko/customer/customer_loyalty/customer_loyalty.snappy.parquet (FILE_FORMAT => EXTERNAL.PARQUET_FORMAT) LIMIT 100;

CREATE OR REPLACE TABLE CUSTOMER_LOYALTY
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(
      INFER_SCHEMA(
        LOCATION=>'@external.frostbyte_raw_stage/tko/customer/customer_loyalty',
        FILE_FORMAT=>'EXTERNAL.PARQUET_FORMAT'
      )
    )
  );

COPY INTO CUSTOMER_LOYALTY
FROM @external.frostbyte_raw_stage/tko/customer/customer_loyalty
FILE_FORMAT = (FORMAT_NAME = EXTERNAL.PARQUET_FORMAT)
MATCH_BY_COLUMN_NAME = CASE_SENSITIVE;

-- Debug: SELECT * FROM CUSTOMER_LOYALTY LIMIT 100;
