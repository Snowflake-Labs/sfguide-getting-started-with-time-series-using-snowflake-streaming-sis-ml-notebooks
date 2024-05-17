/*
SNOWFLAKE ANALYSIS SCRIPT
*/

-- Set role, context, and warehouse
USE ROLE ROLE_HOL_TIMESERIES;
USE HOL_TIMESERIES.ANALYTICS;
USE WAREHOUSE HOL_ANALYTICS_WH;

-- Run Time Series Analysis across various query profiles

/* RAW
Retrieve time series data between an input start time and end time.
*/
SELECT TAGNAME, TIMESTAMP, VALUE
FROM HOL_TIMESERIES.ANALYTICS.TS_TAG_READINGS
WHERE TIMESTAMP >= '2024-01-01 00:00:00'
AND TIMESTAMP < '2024-01-01 00:00:10'
AND TAGNAME = '/IOT/SENSOR/TAG301'
ORDER BY TAGNAME, TIMESTAMP;

-- STATISTICAL AGGREGATES, DISTRIBUTIONS, AND WATERMARKS
/* COUNT AND COUNT DISTINCT
Retrieve count and distinct counts within the time boundary.

COUNT - Count of all values
COUNT DISTINCT - Count of unique values

Note: Counts can work with both varchar and numeric data types.
*/
SELECT TAGNAME, TO_TIMESTAMP_NTZ('2024-01-01 01:00:00') AS TIMESTAMP,
    COUNT(VALUE) AS COUNT_VALUE,
    COUNT(DISTINCT VALUE) AS COUNT_DISTINCT_VALUE
FROM HOL_TIMESERIES.ANALYTICS.TS_TAG_READINGS
WHERE TIMESTAMP > '2024-01-01 00:00:00'
AND TIMESTAMP <= '2024-01-01 01:00:00'
AND TAGNAME = '/IOT/SENSOR/TAG301'
GROUP BY TAGNAME
ORDER BY TAGNAME;

/* MIN/MAX/AVG/SUM/APPROX_PERCENTILE
Retrieve statistical aggregates for the readings within the time boundary.

MIN - Minimum value
MAX - Maximum value
AVG - Average of values (mean)
SUM - Sum of values
PERCENTILE_50 - 50% of values are less than this
PERCENTILE_95 - 95% of values are less than this

Note: Aggregates can work with numerical data types.
*/
SELECT TAGNAME, TO_TIMESTAMP_NTZ('2024-01-01 01:00:00') AS TIMESTAMP,
    MIN(VALUE_NUMERIC) AS MIN_VALUE,
    MAX(VALUE_NUMERIC) AS MAX_VALUE,
    SUM(VALUE_NUMERIC) AS SUM_VALUE,
    AVG(VALUE_NUMERIC) AS AVG_VALUE,
    APPROX_PERCENTILE(VALUE_NUMERIC, 0.5) AS PERCENTILE_50_VALUE,
    APPROX_PERCENTILE(VALUE_NUMERIC, 0.95) AS PERCENTILE_95_VALUE
FROM HOL_TIMESERIES.ANALYTICS.TS_TAG_READINGS 
WHERE TIMESTAMP > '2024-01-01 00:00:00'
AND TIMESTAMP <= '2024-01-01 01:00:00'
AND TAGNAME = '/IOT/SENSOR/TAG301'
GROUP BY TAGNAME
ORDER BY TAGNAME;

/* RELATIVE FREQUENCY
Consider the use case of calculating the frequency and relative frequency of each value within a specific time frame,
to determine how often the value occurs.

Find the value that occurs most frequently within a time frame. 
*/
SELECT 
    TAGNAME,
    VALUE,
    COUNT(VALUE) AS FREQUENCY,
    COUNT(VALUE) / SUM(COUNT(VALUE)) OVER(PARTITION BY TAGNAME) AS RELATIVE_FREQUENCY
FROM HOL_TIMESERIES.ANALYTICS.TS_TAG_READINGS
WHERE TAGNAME IN ('/IOT/SENSOR/TAG501')
AND TIMESTAMP > '2024-01-01 00:00:00'
AND TIMESTAMP <= '2024-01-01 01:00:00'
AND VALUE IS NOT NULL
GROUP BY TAGNAME, VALUE
ORDER BY TAGNAME, FREQUENCY DESC;

/*
INFO: Query Result Data Contract

The following **two** queries are written with a standard return set of columns,
namely **TAGNAME, TIMESTAMP, and VALUE**.

This is a way to structure your query results format if looking to build an
API for time series data, similar to a data contract with consumers.

The TAGNAME is updated to show that a calculation has been applied to the returned values,
and multiple aggregations can be grouped together using unions.
*/

/* DISTRIBUTIONS - sample distributions statistics
Retrieve distribution sample statistics within the time boundary.

STDDEV - Closeness to the mean/average of the distribution.
VARIANCE - Spread between numbers in the time boundary.
KURTOSIS - Measure of outliers occuring.
SKEW - Left (negative) and right (positive) distribution skew.

Note: Distributions can work with numerical data types.
*/
SELECT TAGNAME || '~STDDEV_1HOUR' AS TAGNAME, TO_TIMESTAMP_NTZ('2024-01-01 01:00:00') AS TIMESTAMP, STDDEV(VALUE_NUMERIC) AS VALUE
FROM HOL_TIMESERIES.ANALYTICS.TS_TAG_READINGS
WHERE TIMESTAMP > '2024-01-01 00:00:00'
AND TIMESTAMP <= '2024-01-01 01:00:00'
AND TAGNAME = '/IOT/SENSOR/TAG301'
GROUP BY TAGNAME
UNION ALL
SELECT TAGNAME || '~VARIANCE_1HOUR' AS TAGNAME, TO_TIMESTAMP_NTZ('2024-01-01 01:00:00') AS TIMESTAMP, VARIANCE(VALUE_NUMERIC) AS VALUE
FROM HOL_TIMESERIES.ANALYTICS.TS_TAG_READINGS
WHERE TIMESTAMP > '2024-01-01 00:00:00'
AND TIMESTAMP <= '2024-01-01 01:00:00'
AND TAGNAME = '/IOT/SENSOR/TAG301'
GROUP BY TAGNAME
UNION ALL
SELECT TAGNAME || '~KURTOSIS_1HOUR' AS TAGNAME, TO_TIMESTAMP_NTZ('2024-01-01 01:00:00') AS TIMESTAMP, KURTOSIS(VALUE_NUMERIC) AS VALUE
FROM HOL_TIMESERIES.ANALYTICS.TS_TAG_READINGS
WHERE TIMESTAMP > '2024-01-01 00:00:00'
AND TIMESTAMP <= '2024-01-01 01:00:00'
AND TAGNAME = '/IOT/SENSOR/TAG301'
GROUP BY TAGNAME
UNION ALL
SELECT TAGNAME || '~SKEW_1HOUR' AS TAGNAME, TO_TIMESTAMP_NTZ('2024-01-01 01:00:00') AS TIMESTAMP, SKEW(VALUE_NUMERIC) AS VALUE
FROM HOL_TIMESERIES.ANALYTICS.TS_TAG_READINGS
WHERE TIMESTAMP > '2024-01-01 00:00:00'
AND TIMESTAMP <= '2024-01-01 01:00:00'
AND TAGNAME = '/IOT/SENSOR/TAG301'
GROUP BY TAGNAME
ORDER BY TAGNAME;

/* WATERMARKS
Consider the use case of **determining a sensor variance over time** by calculating the latest (high watermark)
and earliest (low watermark) readings within a time boundary.

Retrieve both the high watermark (latest time stamped value) and low watermark (earliest time stamped value)
readings within the time boundary.

MAX_BY - High Watermark - latest reading in the time boundary
MIN_BY - Low Watermark - earliest reading in the time boundary
*/
SELECT TAGNAME || '~MAX_BY_1HOUR' AS TAGNAME, MAX_BY(TIMESTAMP, TIMESTAMP) AS TIMESTAMP, MAX_BY(VALUE, TIMESTAMP) AS VALUE
FROM HOL_TIMESERIES.ANALYTICS.TS_TAG_READINGS
WHERE TIMESTAMP > '2024-01-01 00:00:00'
AND TIMESTAMP <= '2024-01-01 01:00:00'
AND TAGNAME = '/IOT/SENSOR/TAG301'
GROUP BY TAGNAME
UNION ALL
SELECT TAGNAME || '~MIN_BY_1HOUR' AS TAGNAME, MIN_BY(TIMESTAMP, TIMESTAMP) AS TIMESTAMP, MIN_BY(VALUE, TIMESTAMP) AS VALUE
FROM HOL_TIMESERIES.ANALYTICS.TS_TAG_READINGS
WHERE TIMESTAMP > '2024-01-01 00:00:00'
AND TIMESTAMP <= '2024-01-01 01:00:00'
AND TAGNAME = '/IOT/SENSOR/TAG301'
GROUP BY TAGNAME
ORDER BY TAGNAME;

/*
Time Series Analytics using Window Functions

Window Functions enable aggregates to operate over groups of data,
looking forward and backwards in the ordered data rows,
and returning a single result for each group.

The OVER() clause defines the group of rows used in the calculation.
The PARTITION BY sub-clause allows us to divide that window into sub-windows.
The ORDER BY clause can be used with ASC (ascending) or DESC (descending), and allows ordering of the partition sub-window rows.
*/

/* WINDOW FUNCTIONS - LAG AND LEAD
Consider the use case where you need to analyze the changes in the readings of a specific IoT sensor
over a short period (say 10 seconds) by examining the current, previous, and next values of the readings.

Access data in previous (LAG) or subsequent (LEAD) rows without having to join the table to itself.

LAG - Prior time period value
LEAD - Next time period value
*/
SELECT TAGNAME, TIMESTAMP, VALUE_NUMERIC AS VALUE,
    LAG(VALUE_NUMERIC) OVER (
        PARTITION BY TAGNAME ORDER BY TIMESTAMP) AS LAG_VALUE,
    LEAD(VALUE_NUMERIC) OVER (
        PARTITION BY TAGNAME ORDER BY TIMESTAMP) AS LEAD_VALUE
FROM HOL_TIMESERIES.ANALYTICS.TS_TAG_READINGS
WHERE TIMESTAMP >= '2024-01-01 00:00:00'
AND TIMESTAMP < '2024-01-01 00:00:10'
AND TAGNAME = '/IOT/SENSOR/TAG301'
ORDER BY TAGNAME, TIMESTAMP;

/* FIRST_VALUE AND LAST_VALUE
Consider the use case of change detection where you want to detect any sudden pressure changes in comparison
to initial and final values in a specific time frame.

For this you would use the FIRST_VALUE and LAST_VALUE window functions to retrieve the first and last
values within the time boundary to perform such an analysis.

FIRST_VALUE - First value in the time boundary
LAST_VALUE - Last value in the time boundary
*/
SELECT TAGNAME, TIMESTAMP, VALUE_NUMERIC AS VALUE, 
    FIRST_VALUE(VALUE_NUMERIC) OVER (
        PARTITION BY TAGNAME ORDER BY TIMESTAMP) AS FIRST_VALUE,
    LAST_VALUE(VALUE_NUMERIC) OVER (
        PARTITION BY TAGNAME ORDER BY TIMESTAMP) AS LAST_VALUE
FROM HOL_TIMESERIES.ANALYTICS.TS_TAG_READINGS
WHERE TIMESTAMP >= '2024-01-01 00:00:00'
AND TIMESTAMP < '2024-01-01 00:00:10'
AND TAGNAME = '/IOT/SENSOR/TAG301'
ORDER BY TAGNAME, TIMESTAMP;

/* WINDOW FUNCTIONS - ROWS BETWEEN
Consider the use case, where the data you have second by second sensor reading and you want to compute the
rolling 6 second average of sensor readings over a specific time frame to detect trends and patterns in the data. 

In cases where the data doesn't have any gaps like this one, you can use ROW BETWEEN
window frames to perform these rolling calculations.

Create a rolling AVG for the five preceding and following rows, inclusive of the current row.

ROW_AVG_PRECEDING - Rolling AVG from 5 preceding rows and current row
ROW_AVG_FOLLOWING - Rolling AVG from current row and 5 following rows
*/
SELECT TAGNAME, TIMESTAMP, VALUE_NUMERIC AS VALUE,
    AVG(VALUE_NUMERIC) OVER (
        PARTITION BY TAGNAME ORDER BY TIMESTAMP
        ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS ROW_AVG_PRECEDING,
    AVG(VALUE_NUMERIC) OVER (
        PARTITION BY TAGNAME ORDER BY TIMESTAMP
        ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING) AS ROW_AVG_FOLLOWING
FROM HOL_TIMESERIES.ANALYTICS.TS_TAG_READINGS
WHERE TIMESTAMP >= '2024-01-01 00:00:00'
AND TIMESTAMP < '2024-01-01 00:01:00'
AND TAGNAME = '/IOT/SENSOR/TAG301'
ORDER BY TAGNAME, TIMESTAMP;

/* SENSOR WITH TIME GAPS
Now assume a scenario, where there are time gaps or missing data received from a sensor.
Such as a sensor that sends roughly every 5 seconds, or if a sensor experiences a fault.

In this example I am using DATE_PART to exclude seconds 20, 45, and 55 from the data.
*/
SELECT TAGNAME, TIMESTAMP, VALUE_NUMERIC AS VALUE
FROM HOL_TIMESERIES.ANALYTICS.TS_TAG_READINGS
WHERE TIMESTAMP >= '2024-01-01 00:00:00'
AND TIMESTAMP < '2024-01-01 00:02:00'
AND TAGNAME = '/IOT/SENSOR/TAG101'
AND DATE_PART('SECOND', TIMESTAMP) NOT IN (20, 45, 55)
ORDER BY TAGNAME, TIMESTAMP;

/* WINDOW FUNCTIONS - RANGE BETWEEN
Now say you want to perform an aggregation to calculate the 1 minute rolling average of sensor readings,
over a specific time frame to detect trends and patterns in the data. 

ROWS BETWEEN may NOT yield correct results, as the number of rows that make up the 1 minute interval is inconsistent.

This is where **RANGE BETWEEN** can be used with intervals of time that can be added or subtracted from timestamps.

RANGE BETWEEN differs from ROWS BETWEEN in that it can:
* Handle time gaps in data being analyzed.
    For example, if a sensor is faulty or send data at incosistent intervals.

* Allow for reporting frequencies that differ from the data frequency.
    For example, data at 5 second frequency that you want to aggregate the prior 1 minute.

Create a rolling AVG and SUM for the time INTERVAL 1 minutes preceding, inclusive of the current row.

INTERVAL - 1 MIN AVG and SUM preceding the current row - showing differences between RANGE BETWEEN AND ROWS BETWEEN
*/
SELECT TAGNAME, TIMESTAMP, VALUE_NUMERIC AS VALUE,
    AVG(VALUE_NUMERIC) OVER (
        PARTITION BY TAGNAME ORDER BY TIMESTAMP
        RANGE BETWEEN INTERVAL '1 MIN' PRECEDING AND CURRENT ROW) AS RANGE_AVG_1MIN,
    AVG(VALUE_NUMERIC) OVER (
        PARTITION BY TAGNAME ORDER BY TIMESTAMP
        ROWS BETWEEN 12 PRECEDING AND CURRENT ROW) AS ROW_AVG_1MIN,
    SUM(VALUE_NUMERIC) OVER (
        PARTITION BY TAGNAME ORDER BY TIMESTAMP
        RANGE BETWEEN INTERVAL '1 MIN' PRECEDING AND CURRENT ROW) AS RANGE_SUM_1MIN,
    SUM(VALUE_NUMERIC) OVER (
        PARTITION BY TAGNAME ORDER BY TIMESTAMP
        ROWS BETWEEN 12 PRECEDING AND CURRENT ROW) AS ROW_SUM_1MIN
FROM HOL_TIMESERIES.ANALYTICS.TS_TAG_READINGS
WHERE TIMESTAMP >= '2024-01-01 00:00:00'
AND TIMESTAMP <= '2024-01-01 01:00:00'
AND DATE_PART('SECOND', TIMESTAMP) NOT IN (20, 45, 55)
AND TAGNAME = '/IOT/SENSOR/TAG101'
ORDER BY TAGNAME, TIMESTAMP;

/*
CHART: Rolling 1 MIN Average and Sum - showing differences between RANGE BETWEEN and ROWS BETWEEN

1. Select the `Chart` sub tab below the worksheet.
2. Under Data select the `VALUE` column and set the Aggregation to `Max`.
3. Select `+ Add column` and select `RANGE_AVG_1MIN` and set Aggregation to `Max`.
4. Select `+ Add column` and select `ROW_AVG_1MIN` and set Aggregation to `Max`.
*/

/* WINDOW FUNCTIONS - RANGE BETWEEN
Let's expand on RANGE BETWEEN and create a rolling AVG and SUM for the time **INTERVAL** five minutes preceding, inclusive of the current row.

INTERVAL - 5 MIN AVG and SUM preceding the current row
*/
SELECT TAGNAME, TIMESTAMP, VALUE_NUMERIC AS VALUE,
    AVG(VALUE_NUMERIC) OVER (
        PARTITION BY TAGNAME ORDER BY TIMESTAMP
        RANGE BETWEEN INTERVAL '5 MIN' PRECEDING AND CURRENT ROW) AS RANGE_AVG_5MIN,
    SUM(VALUE_NUMERIC) OVER (
        PARTITION BY TAGNAME ORDER BY TIMESTAMP
        RANGE BETWEEN INTERVAL '5 MIN' PRECEDING AND CURRENT ROW) AS RANGE_SUM_5MIN
FROM HOL_TIMESERIES.ANALYTICS.TS_TAG_READINGS
WHERE TIMESTAMP >= '2024-01-01 00:00:00'
AND TIMESTAMP <= '2024-01-01 01:00:00'
AND TAGNAME = '/IOT/SENSOR/TAG101'
ORDER BY TAGNAME, TIMESTAMP;

/*
CHART: Rolling 5 MIN Average

1. Select the `Chart` sub tab below the worksheet.
2. Under Data select the `VALUE` column and set the Aggregation to `Max`.
3. Select `+ Add column` and select `RANGE_AVG_5MIN` and set Aggregation to `Max`.
*/

/* DOWNSAMPLING
Downsampling is used to decrease the frequency of time samples, such as from seconds to minutes,
by placing time series data into fixed time intervals using aggregate operations on the existing values within each time interval.
*/

/* TIME BINNING - 5 min AGGREGATE with START and END label
Create a downsampled time series data set with 5 minute aggregates, showing the START and END timestamp label of each interval.

COUNT - Count of values within the time bin
SUM - Sum of values within the time bin
AVG - Average of values (mean) within the time bin
PERCENTILE_95 - 95% of values are less than this within the time bin
*/
SELECT TAGNAME,
    TIME_SLICE(TIMESTAMP, 5, 'MINUTE', 'START') AS START_TIMESTAMP,
    TIME_SLICE(TIMESTAMP, 5, 'MINUTE', 'END') AS END_TIMESTAMP,
    COUNT(*) AS COUNT_VALUE,
    SUM(VALUE_NUMERIC) AS SUM_VALUE,
    AVG(VALUE_NUMERIC) AS AVG_VALUE,
    APPROX_PERCENTILE(VALUE_NUMERIC, 0.95) AS PERCENTILE_95_VALUE
FROM HOL_TIMESERIES.ANALYTICS.TS_TAG_READINGS
WHERE TIMESTAMP >= '2024-01-01 00:00:00'
AND TIMESTAMP < '2024-01-01 01:00:00'
AND TAGNAME = '/IOT/SENSOR/TAG301'
GROUP BY TIME_SLICE(TIMESTAMP, 5, 'MINUTE', 'START'), TIME_SLICE(TIMESTAMP, 5, 'MINUTE', 'END'), TAGNAME
ORDER BY TAGNAME, START_TIMESTAMP;

/* ASOF JOIN - Align a 1 second tag with a 5 second tag
Using the `ASOF JOIN`, join two data sets by applying a matching condition to pair closely aligned timestamps and values.
*/
SELECT ONE_SEC.TAGNAME AS ONE_SEC_TAGNAME, ONE_SEC.TIMESTAMP AS ONE_SEC_TIMESTAMP, ONE_SEC.VALUE_NUMERIC AS ONE_SEC_VALUE, FIVE_SEC.VALUE_NUMERIC AS FIVE_SEC_VALUE, FIVE_SEC.TAGNAME AS FIVE_SEC_TAGNAME, FIVE_SEC.TIMESTAMP AS FIVE_SEC_TIMESTAMP
FROM HOL_TIMESERIES.ANALYTICS.TS_TAG_READINGS ONE_SEC
ASOF JOIN (
    -- 5 sec tag data
    SELECT TAGNAME, TIMESTAMP, VALUE_NUMERIC
    FROM HOL_TIMESERIES.ANALYTICS.TS_TAG_READINGS
    WHERE TAGNAME = '/IOT/SENSOR/TAG101'
    ) FIVE_SEC
MATCH_CONDITION(ONE_SEC.TIMESTAMP >= FIVE_SEC.TIMESTAMP)
WHERE ONE_SEC.TAGNAME = '/IOT/SENSOR/TAG301'
AND ONE_SEC.TIMESTAMP >= '2024-01-01 00:00:00'
AND ONE_SEC.TIMESTAMP < '2024-01-01 00:01:00'
ORDER BY ONE_SEC.TIMESTAMP;

/* TIME GAP FILLING
Generate timestamps given a start and end time boundary, and join to a tag with less frequent values.

TIME_PERIODS - A variable passed into the query to determine the number of time stamps generated for gap filling.
*/
SET TIME_PERIODS = (SELECT TIMESTAMPDIFF('SECOND', '2024-01-01 00:00:00'::TIMESTAMP_NTZ, '2024-01-01 00:00:00'::TIMESTAMP_NTZ + INTERVAL '1 MINUTE'));

-- LAST OBSERVED VALUE CARRIED FORWARD (LOCF) - IGNORE NULLS
WITH TIMES AS (
    SELECT
    DATEADD('SECOND', ROW_NUMBER() OVER (ORDER BY SEQ8()) - 1, '2024-01-01')::TIMESTAMP_NTZ AS TIMESTAMP,
    '/IOT/SENSOR/TAG101' AS TAGNAME
    FROM TABLE(GENERATOR(ROWCOUNT => $TIME_PERIODS))
),
DATA AS (
    SELECT TAGNAME, TIMESTAMP, VALUE_NUMERIC AS VALUE,
    FROM HOL_TIMESERIES.ANALYTICS.TS_TAG_READINGS
    WHERE TIMESTAMP >= '2024-01-01 00:00:00'
    AND TIMESTAMP < '2024-01-01 00:01:00'
    AND TAGNAME = '/IOT/SENSOR/TAG101'
)
SELECT TIMES.TIMESTAMP,
    A.TAGNAME AS TAGNAME,
    L.VALUE,
    A.VALUE AS LOCF_VALUE
FROM TIMES
LEFT JOIN DATA L ON TIMES.TIMESTAMP = L.TIMESTAMP AND TIMES.TAGNAME = L.TAGNAME
ASOF JOIN DATA A MATCH_CONDITION(TIMES.TIMESTAMP >= A.TIMESTAMP) ON TIMES.TAGNAME = A.TAGNAME
ORDER BY TAGNAME, TIMESTAMP;

/*
ANALYSIS SCRIPT COMPLETED
*/