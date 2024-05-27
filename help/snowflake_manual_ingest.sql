/*##### MANUAL INGEST SCRIPT #####*/

-- Set role, context, and warehouse
USE ROLE ROLE_HOL_TIMESERIES;
USE SCHEMA HOL_TIMESERIES.STAGING;
USE WAREHOUSE HOL_TRANSFORM_WH;

-- Create stage table
CREATE OR REPLACE TABLE HOL_TIMESERIES.STAGING.RAW_TS_STAGE_DATA (
    READING VARCHAR,
    NAMESPACE VARCHAR,
    TAGNAME VARCHAR,
    TIMESTAMP VARCHAR,
    VALUE VARCHAR,
    UNITS VARCHAR,
    DATATYPE VARCHAR
);

-- Copy data into stage table
COPY INTO HOL_TIMESERIES.STAGING.RAW_TS_STAGE_DATA
FROM s3://sfquickstarts/vhol_getting_started_with_time_series/STREAM_DATA.csv
FILE_FORMAT = (TYPE = 'CSV', SKIP_HEADER = 1);

-- Transform and insert data from stage table to stream format
INSERT INTO HOL_TIMESERIES.STAGING.RAW_TS_IOTSTREAM_DATA
SELECT
    OBJECT_CONSTRUCT(
        'LogAppendTime', DATE_PART(epoch_millisecond, SYSDATE())::VARCHAR,
        'headers', OBJECT_CONSTRUCT('namespace', 'IOT', 'source', 'STREAM_DATA.csv', 'speed', 'MANUAL'),
        'offset', READING::VARCHAR,
        'partition', 1::VARCHAR,
        'topic', 'time-series'
    ) AS RECORD_METADATA,
    OBJECT_CONSTRUCT(
        'datatype', DATATYPE,
        'tagname', TAGNAME,
        'timestamp', DATE_PART(epoch_second, TIMESTAMP::VARCHAR::TIMESTAMP_NTZ)::VARCHAR,
        'units', UNITS,
        'value', VALUE
    ) AS RECORD_CONTENT
FROM HOL_TIMESERIES.STAGING.RAW_TS_STAGE_DATA
ORDER BY READING;

/*##### MANUAL INGEST SCRIPT #####*/