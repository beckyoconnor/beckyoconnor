/*-----------------------------------------------------------------------------
Hands-On Lab: Data Engineering with Snowpark
Script:       08_incremental_process.sql
Author:       Jeremiah Hansen
Last Updated: 1/2/2023
-----------------------------------------------------------------------------*/

USE ROLE HOL_ROLE;
USE WAREHOUSE HOL_WH;
USE DATABASE HOL_DB;


-- ----------------------------------------------------------------------------
-- Step #1: Add new/remaining order data
-- ----------------------------------------------------------------------------

USE SCHEMA RAW_POS;

COPY INTO ORDER_HEADER
FROM @external.frostbyte_raw_stage/tko/pos/order_header/year=2022
FILE_FORMAT = (FORMAT_NAME = EXTERNAL.PARQUET_FORMAT)
MATCH_BY_COLUMN_NAME = CASE_SENSITIVE;

COPY INTO ORDER_DETAIL
FROM @external.frostbyte_raw_stage/tko/pos/order_detail/year=2022
FILE_FORMAT = (FORMAT_NAME = EXTERNAL.PARQUET_FORMAT)
MATCH_BY_COLUMN_NAME = CASE_SENSITIVE;


-- ----------------------------------------------------------------------------
-- Step #2: Execute the tasks
-- ----------------------------------------------------------------------------

USE SCHEMA HARMONIZED;

EXECUTE TASK ORDERS_UPDATE_TASK;


-- ----------------------------------------------------------------------------
-- Step #3: Monitor tasks in Snowsight
-- ----------------------------------------------------------------------------

-- TODO: Add Snowsight details here
-- https://docs.snowflake.com/en/user-guide/ui-snowsight-tasks.html
