USE ROLE ROLE_HOL_TIMESERIES;
USE SCHEMA HOL_TIMESERIES.STAGING;

-- Utilities
CREATE OR REPLACE STAGE HOL_TIMESERIES.STAGING.STAGE_TS_UTIL
DIRECTORY = (ENABLE = TRUE, REFRESH_ON_CREATE = TRUE);

-- WITSML Files
CREATE OR REPLACE STAGE HOL_TIMESERIES.STAGING.STAGE_TS_WITSML
DIRECTORY = (ENABLE = TRUE, REFRESH_ON_CREATE = TRUE);

-- HDF5 Files
CREATE OR REPLACE STAGE HOL_TIMESERIES.STAGING.STAGE_TS_HDF5
DIRECTORY = (ENABLE = TRUE, REFRESH_ON_CREATE = TRUE);

-- File Formats
CREATE OR REPLACE FILE FORMAT HOL_TIMESERIES.STAGING.FILE_FORMAT_XML
    TYPE = XML
    COMPRESSION = NONE
    IGNORE_UTF8_ERRORS = TRUE
    PRESERVE_SPACE = FALSE
    STRIP_OUTER_ELEMENT = FALSE
    DISABLE_SNOWFLAKE_DATA = FALSE
    DISABLE_AUTO_CONVERT = FALSE
    REPLACE_INVALID_CHARACTERS = FALSE
    SKIP_BYTE_ORDER_MARK = TRUE;

-- Stage Tables
-- WITSML
CREATE OR REPLACE TABLE HOL_TIMESERIES.STAGING.RAW_TS_WITSML_DATA (
    RECORD_CONTENT VARIANT
);

-- IOTSTREAM
CREATE OR REPLACE TABLE HOL_TIMESERIES.STAGING.RAW_TS_IOTSTREAM_DATA (
    RECORD_CONTENT VARIANT
);

-- Configure Snoflake CLI and IOT Stream
/*
Test Snowflake CLI
1) Update Snowflake CLI config.toml
snow --config-file="/Users/nbirch/GitHub/dc-gh-nathan/sfguide-getting-started-snowflake-time-series/.snowflake/config.toml" connection test --connection="hol-timeseries"


*/

-- UPLOAD WITSML with Snowflake CLI
/*
WITSML: https://en.wikipedia.org/wiki/Wellsite_information_transfer_standard_markup_language
Wellsite information transfer standard markup language

WITSML is a standard for transmitting technical data between organisations in the petroleum industry. It continues to be developed by an Energistics facilitated Special Interest Group to develop XML standards for drilling, completions, and interventions data exchange. Organizations for which WITSML is targeted include energy companies, service companies, drilling contractors, application vendors and regulatory agencies.

A modern drilling rig or offshore platform uses a diverse array of specialist contractors, each of whom need to communicate data to the oil company operating the rig, and to each other. Historically this was done with serial transfer of ASCII data.
*/

LIST @STAGE_TS_WITSML;
LIST @STAGE_TS_HDF5;

SELECT $1 FROM @STAGE_TS_WITSML LIMIT 10;

-- Ingest Stage Data
-- WITSML Data
COPY INTO HOL_TIMESERIES.STAGING.RAW_TS_WITSML_DATA
FROM @STAGE_TS_WITSML
FILE_FORMAT = HOL_TIMESERIES.STAGING.FILE_FORMAT_XML;

SELECT * FROM HOL_TIMESERIES.STAGING.RAW_TS_WITSML_DATA LIMIT 10;

-- Stream Data
/*
Run streaming ingest client
*/

show channels;
