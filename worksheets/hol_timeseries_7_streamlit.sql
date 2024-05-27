/*##### STREAMLIT SCRIPT #####*/

-- Set role, context, and warehouse
USE ROLE ROLE_HOL_TIMESERIES;
USE SCHEMA HOL_TIMESERIES.ANALYTICS;
USE WAREHOUSE HOL_ANALYTICS_WH;

-- Create a stage for Streamlit
CREATE OR REPLACE STAGE HOL_TIMESERIES.ANALYTICS.STAGE_TS_STREAMLIT
DIRECTORY = (ENABLE = TRUE, REFRESH_ON_CREATE = TRUE)
ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

/*###### EXTERNAL ACTIVITY #####
Use Snowflake CLI to upload Streamlit app:
$ conda activate hol-timeseries
$ snow --config-file=".snowflake/config.toml" streamlit deploy --replace --project "streamlit" --connection="hol-timeseries-streamlit"
##############################*/

/*##### STREAMLIT SCRIPT #####*/