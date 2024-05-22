/*
SNOWFLAKE FUNCTION QUERIES SCRIPT
*/

-- Set role, context, and warehouse
USE ROLE ROLE_HOL_TIMESERIES;
USE HOL_TIMESERIES.ANALYTICS;
USE WAREHOUSE HOL_ANALYTICS_WH;

/* INTERPOLATE TABLE FUNCTION
The Interpolation table function will return both the linear interpolated values and last observed value carried forward (LOCF).
*/
SELECT * FROM TABLE(HOL_TIMESERIES.ANALYTICS.FUNCTION_TS_INTERPOLATE('/IOT/SENSOR/TAG401', '2024-01-01 12:10:00'::TIMESTAMP_NTZ, '2024-01-01 13:10:00'::TIMESTAMP_NTZ, 10, 362)) ORDER BY TAGNAME, TIMESTAMP;

/*
CHART: Interpolation - Linear and LOCF

1. Select the `Chart` sub tab below the worksheet.
2. Under Data select `TIMESTAMP` and set Bucketing to `Second`
3. Under Data select `INTERP_VALUE` and set the Aggregation to `Max`.
4. Select `+ Add column` and select `LOCF_VALUE` and set Aggregation to `Max`.

The chart will display both LINEAR and LOCF for interpolated values between data points.
*/

/* INTERPOLATE PROCEDURE - LOCF
The Interpolation Procedure will accept a start time and end time, along with a bucket interval size in seconds.

It will then calculate the number of buckets within the time boundary, and call the Interpolate table function.

Call Interpolate Procedure with Taglist, Start Time, End Time, and Intervals, with `LOCF` Interpolate type.
*/
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

The chart will display a LOCF value where the prior value is interpolated between data points.
*/

/* INTERPOLATE PROCEDURE - LINEAR
Similar to the LOCF interpolation procedure call, this will create a Linear Interpolation table.

Call Interpolate Procedure with Taglist, Start Time, End Time, and Intervals, with `LINEAR` Interpolate type.
*/
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
    'LINEAR'
);

/*
CHART: Interpolation - LINEAR

1. Select the `Chart` sub tab below the worksheet.
2. Under Data select `VALUE` and set the Aggregation to `Max`.

The chart will display a smoother LINEAR interpolated value between data points.
*/

/*
LTTB Query

The Largest Triangle Three Buckets (LTTB) algorithm is a time series downsampling algorithm that
reduces the number of visual data points, whilst retaining the shape and variability of the time series data.

Starting with a RAW query we can see the LTTB function in action, 
where the function will downsample two hours of data for a one second tag,
7200 data points downsampled to 500 whilst keeping the shape and variability of the values.
*/

/* RAW - 2 HOURS OF 1 SEC DATA
Source of downsample - 7200 data points
*/
SELECT TAGNAME, TIMESTAMP, VALUE_NUMERIC as VALUE
FROM HOL_TIMESERIES.ANALYTICS.TS_TAG_READINGS
WHERE TIMESTAMP > '2024-01-09 21:00:00'
AND TIMESTAMP <= '2024-01-09 23:00:00'
AND TAGNAME = '/IOT/SENSOR/TAG301'
ORDER BY TAGNAME, TIMESTAMP;

/*
CHART: Raw

1. Select the `Chart` sub tab below the worksheet.
2. Under Data select `VALUE` and set the Aggregation to `Max`.
3. Under Data select `TIMESTAMP` and set the Bucketing to `Second`.

7200 Data Points
*/

/* LTTB - DOWNSAMPLE TO 500 DATA POINTS
We can now pass the same data into the LTTB table function and request 500 data points to be returned.

The DATA subquery sets up the data set, and this is cross joined with the LTTB table function,
with an input of TIMESTAMP, VALUE, and the downsample size of 500.
*/
SELECT DATA.TAGNAME, LTTB.TIMESTAMP::VARCHAR::TIMESTAMP_NTZ AS TIMESTAMP, LTTB.VALUE 
FROM (
    SELECT TAGNAME, TIMESTAMP, VALUE_NUMERIC as VALUE
    FROM HOL_TIMESERIES.ANALYTICS.TS_TAG_READINGS
    WHERE TIMESTAMP > '2024-01-09 21:00:00'
    AND TIMESTAMP <= '2024-01-09 23:00:00'
    AND TAGNAME = '/IOT/SENSOR/TAG301'
) AS DATA 
CROSS JOIN TABLE(HOL_TIMESERIES.ANALYTICS.FUNCTION_TS_LTTB(DATE_PART(EPOCH_NANOSECOND, DATA.TIMESTAMP), DATA.VALUE, 500) OVER (PARTITION BY DATA.TAGNAME ORDER BY DATA.TIMESTAMP)) AS LTTB
ORDER BY TAGNAME, TIMESTAMP;

/*
CHART: LTTB Query

1. Select the `Chart` sub tab below the worksheet.
2. Under Data select `VALUE` and set the Aggregation to `Max`.
3. Under Data select `TIMESTAMP` and set the Bucketing to `Second`.

500 Data Points - The shape and variability of the values are retained, when compared to the 7200 data point RAW chart.
*/

/*
FUNCTION QUERIES SCRIPT COMPLETED
*/