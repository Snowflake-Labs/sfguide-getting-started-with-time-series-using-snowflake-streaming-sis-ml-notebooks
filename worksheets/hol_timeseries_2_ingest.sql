/*##### INGEST SCRIPT #####*/

/*##############################
-- Staging Table - START
##############################*/

-- Set role, context, and warehouse
USE ROLE ROLE_HOL_TIMESERIES;
USE SCHEMA HOL_TIMESERIES.STAGING;
USE WAREHOUSE HOL_TRANSFORM_WH;

-- Setup staging tables - RAW_TS_IOTSTREAM_DATA
CREATE OR REPLACE TABLE HOL_TIMESERIES.STAGING.RAW_TS_IOTSTREAM_DATA (
    RECORD_METADATA VARIANT,
    RECORD_CONTENT VARIANT
)
CHANGE_TRACKING = TRUE
COMMENT = 'IOTSTREAM staging table.'
;

/*##############################
-- Staging Table - END
##############################*/

/*###### EXTERNAL ACTIVITY #####
Run IOT Stream client Test - /iotstream/Test.sh
##############################*/

-- Show stream channels connections
SHOW CHANNELS;

/*###### EXTERNAL ACTIVITY #####
Run IOT Stream client Load - /iotstream/Run_MAX.sh
##############################*/

-- Check stream table data
SELECT * FROM HOL_TIMESERIES.STAGING.RAW_TS_IOTSTREAM_DATA LIMIT 10;

/*##### INGEST SCRIPT #####*/