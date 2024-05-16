/*
SNOWFLAKE FUNCTION QUERIES SCRIPT
*/

-- Set role, context, and warehouse
USE ROLE ROLE_HOL_TIMESERIES;
USE HOL_TIMESERIES.ANALYTICS;
USE WAREHOUSE HOL_ANALYTICS_WH;

-- Directly Call Table Function
SELECT * FROM TABLE(HOL_TIMESERIES.ANALYTICS.FUNCTION_TS_INTERPOLATE('/IOT/SENSOR/TAG401', '2024-01-01 12:10:00'::TIMESTAMP_NTZ, '2024-01-01 13:10:00'::TIMESTAMP_NTZ, 10, 362)) ORDER BY TAGNAME, TIMESTAMP;

/*
CHART: Interpolation - Linear and LOCF

1. Select the `Chart` sub tab below the worksheet.
2. Under Data select `INTERP_VALUE` and set the Aggregation to `Max`.
3. Select `+ Add column` and select `LOCF_VALUE` and set Aggregation to `Max`.
*/

-- Call Interpolate Procedure with Taglist, Start Time, End Time, and Intervals - LOCF Interpolate
CALL HOL_TIMESERIES.ANALYTICS.PROCEDURE_TS_INTERPOLATE(
    -- V_TAGLIST
    '/IOT/SENSOR/TAG401',
    -- V_FROM_TIME
    '2024-01-01 12:10:00',
    -- V_TO_TIME
    '2024-01-01 13:10:00',
    -- V_INTERVAL
    10,
    -- V_INTERP_TYPE
    'LOCF'
);

/*
CHART: Interpolation - LOCF

1. Select the `Chart` sub tab below the worksheet.
2. Under Data select `VALUE` and set the Aggregation to `Max`.
*/

-- Call Interpolate Procedure with Taglist, Start Time, End Time, and Intervals - LINEAR Interpolate
CALL HOL_TIMESERIES.ANALYTICS.PROCEDURE_TS_INTERPOLATE(
    -- V_TAGLIST
    '/IOT/SENSOR/TAG401',
    -- V_FROM_TIME
    '2024-01-01 12:10:00',
    -- V_TO_TIME
    '2024-01-01 13:10:00',
    -- V_INTERVAL
    10,
    -- V_INTERP_TYPE
    'INTERP'
);

/*
CHART: Interpolation - LINEAR

1. Select the `Chart` sub tab below the worksheet.
2. Under Data select `VALUE` and set the Aggregation to `Max`.
*/

-- LTTB
SELECT DATA.TAGNAME, LTTB.TIMESTAMP::VARCHAR::TIMESTAMP_NTZ AS TIMESTAMP, LTTB.VALUE 
FROM (
    SELECT TAGNAME, TIMESTAMP, VALUE_NUMERIC AS VALUE
    FROM HOL_TIMESERIES.ANALYTICS.TS_TAG_READINGS
    WHERE TIMESTAMP > '2024-01-01 00:00:00'
    AND TIMESTAMP <= '2024-02-01 00:00:30'
    AND TAGNAME = '/IOT/SENSOR/TAG301'
) AS DATA 
CROSS JOIN TABLE(HOL_TIMESERIES.ANALYTICS.FUNCTION_TS_LTTB(DATE_PART(EPOCH_NANOSECOND, DATA.TIMESTAMP), DATA.VALUE, 500) OVER (PARTITION BY DATA.TAGNAME ORDER BY DATA.TIMESTAMP)) AS LTTB
ORDER BY TAGNAME, TIMESTAMP;

/*
CHART: LTTB Query

1. Select the `Chart` sub tab below the worksheet.
2. Under Data select `VALUE` and set the Aggregation to `Max`.
3. Under Data select `TIMESTAMP` and set the Bucketing to `Minute`. 
*/

/*
FUNCTION QUERIES SCRIPT COMPLETED
*/