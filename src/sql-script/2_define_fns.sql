
-- The following resources are assumed and pre-existing
use role public;
use warehouse DEMO_BUILD_WH;
use schema &APP_DB_database.public;


-- =========================
PUT file://./src/python/pneumonia_image_trainer.py @lib_stg/scripts 
    auto_compress = false
    overwrite = true;

create or replace procedure train_pneumonia_identification_model(row_limit integer ,stage_path varchar ,staged_model_flname varchar ,epochs int)
    returns variant
    language python
    runtime_version = '3.8'
    packages = ('snowflake-snowpark-python','numpy', 'pandas', 'snowflake-snowpark-python' ,'tensorflow' ,'scikit-learn')
    imports = ('@lib_stg/scripts/pneumonia_image_trainer.py')
    handler = 'pneumonia_image_trainer.main'
    ;

-- =========================

PUT file://./src/python/pneumonia_image_inference.py @lib_stg/scripts 
    auto_compress = false
    overwrite = true;

-- create or replace procedure infer_pneumonia(IMAGE_ARRAY_SHAPE_0 integer ,IMAGE_ARRAY_SHAPE_1 integer ,RESIZED_FEATURE varchar)
--     returns variant
--     language python
--     runtime_version = '3.8'
--     packages = ('snowflake-snowpark-python','numpy', 'pandas', 'snowflake-snowpark-python' ,'tensorflow' ,'scikit-learn')
--     imports = ('@lib_stg/scripts/pneumonia_image_inference.py'
--         ,'@model_stg/pneumonia_model.joblib')
--     handler = 'pneumonia_image_inference.main'
--     ;