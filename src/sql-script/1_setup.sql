
-- The following resources are assumed and pre-existing
use warehouse &SNOW_CONN_warehouse;

-- =========================
-- This solution requires snowpark optimized warehouse for image training purpose.s
-- =========================
use role accountadmin;

create or replace warehouse snowopt_wh with
    WAREHOUSE_TYPE = 'SNOWPARK-OPTIMIZED'
    WAREHOUSE_SIZE = 'MEDIUM'
    AUTO_RESUME = TRUE
    AUTO_SUSPEND = 300
    COMMENT = 'warehouse created as part of dicom industry solution usecase.'
;
    
grant ALL PRIVILEGES on warehouse snowopt_wh
    to role public;

-- =========================
-- This script is used to configure the base resources that will be used by
-- the demo
-- =========================
use role sysadmin;

create or replace database INDSOL_DICOM_DB
    comment = 'used for demonstrating DICOM image processing demo';

-- Transfer ownership
grant ownership on database INDSOL_DICOM_DB
    to role public;

grant ownership  on schema INDSOL_DICOM_DB.public
    to role public;

grant all privileges  on database INDSOL_DICOM_DB
    to role public;

grant all privileges  on schema INDSOL_DICOM_DB.public
    to role public;
    
-- =========================
-- Define stages
-- =========================
use role public;
use schema INDSOL_DICOM_DB.public;

create or replace stage lib_stg
    comment = 'used for holding libraries and other core artifacts.';

create or replace stage data_stg encryption = (type = 'SNOWFLAKE_SSE')
    directory = ( enable = true )
    comment = 'used for holding data.';

drop stage data_stg;

create or replace stage model_stg
    comment = 'used for holding ml models.';


-- =========================
-- Define tables
-- =========================
create or replace transient table image_parsed_raw (
    seq_no number
    ,image_filepath varchar
    ,parsing_status boolean
    ,parsing_exception varchar
    ,class_label varchar
    ,class_label_num int
    ,image_array_shape_0 int
    ,image_array_shape_1 int
    ,image_array variant
    ,normalized_image_array variant
    ,resized_feature variant
    -- ,inserted_at timestamp default current_timestamp()
)
comment = 'Used for storing parsed images as array'
;