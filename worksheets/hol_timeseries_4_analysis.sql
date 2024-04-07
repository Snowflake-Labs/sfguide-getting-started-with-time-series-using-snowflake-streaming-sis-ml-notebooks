-- Setup Reporting Views
USE ROLE ROLE_HOL_TIMESERIES;
USE HOL_TIMESERIES.ANALYTICS;

-- Tag Reference View
CREATE OR REPLACE VIEW HOL_TIMESERIES.ANALYTICS.TS_TAG_REFERENCE AS
SELECT
    META.NAMESPACE,
    META.TAGNAME,
    META.TAGALIAS,
    META.TAGDESCRIPTION,
    META.TAGUOM,
    META.TAGDATATYPE
FROM HOL_TIMESERIES.TRANSFORM.TS_TAG_METADATA META;

-- Review Tag Reference
SELECT * FROM HOL_TIMESERIES.ANALYTICS.TS_TAG_REFERENCE;

-- Tag Readings View
CREATE OR REPLACE VIEW HOL_TIMESERIES.ANALYTICS.TS_TAG_READINGS AS
SELECT
    META.TAGNAME,
    READ.TS AS TIMESTAMP,
    READ.VAL AS VALUE,
    READ.VAL_NUMERIC AS VALUE_NUMERIC
FROM HOL_TIMESERIES.TRANSFORM.TS_TAG_METADATA META
INNER JOIN HOL_TIMESERIES.TRANSFORM.TS_TAG_READINGS READ
ON META.TAGKEY = READ.TAGKEY;

-- Review Tag Readings
SELECT * FROM HOL_TIMESERIES.ANALYTICS.TS_TAG_READINGS;


-- Run Time Series Analysis across various query profiles

-- RAW DATA
SELECT tagname, timestamp, value, NULL AS value_numeric
FROM HOL_TIMESERIES.ANALYTICS.TS_TAG_READINGS
WHERE timestamp > '2000-03-26 12:45:37' 
AND timestamp <= '2024-03-26 14:45:37' 
AND tagname = '/WITSML/NO 15/9-F-7/DEPTH' 
ORDER BY tagname, timestamp
;

-- HI WATER
SELECT tagname, to_timestamp('2024-03-26 14:47:55') AS timestamp, MAX_BY(value, timestamp) AS value, MAX_BY(value_numeric, timestamp) AS value_numeric 
FROM HOL_TIMESERIES.ANALYTICS.TS_TAG_READINGS
WHERE timestamp > '2000-03-26 08:47:55' 
AND timestamp <= '2024-03-26 14:47:55' 
AND tagname = '/WITSML/NO 15/9-F-7/DEPTH' 
GROUP BY tagname 
ORDER BY tagname, timestamp
;

-- LOW WATER
SELECT tagname, to_timestamp('2024-03-26 14:47:55') AS timestamp, MIN_BY(value, timestamp) AS value, MIN_BY(value_numeric, timestamp) AS value_numeric 
FROM HOL_TIMESERIES.ANALYTICS.TS_TAG_READINGS
WHERE timestamp > '2000-03-26 08:47:55' 
AND timestamp <= '2024-03-26 14:47:55' 
AND tagname = '/WITSML/NO 15/9-F-7/DEPTH' 
GROUP BY tagname 
ORDER BY tagname, timestamp
;

-- DOWNSAMPLING / RESAMPLING

-- BINNING
SELECT tagname, TIME_SLICE(DATEADD(MILLISECOND, -1, timestamp), 15, 'MINUTE', 'END') AS timestamp, NULL AS value, APPROX_PERCENTILE(value_numeric, 0.5) AS value_numeric
FROM HOL_TIMESERIES.ANALYTICS.TS_TAG_READINGS 
WHERE timestamp > '2000-03-26 02:58:38' AND timestamp <= '2024-03-28 14:58:38' 
AND tagname IN ('/WITSML/NO 15/9-F-7/DEPTH') 
GROUP BY TIME_SLICE(DATEADD(MILLISECOND, -1, timestamp), 15, 'MINUTE', 'END'), tagname 
ORDER BY tagname, timestamp
;

-- FIRST_VALUE / LAST_VALUE
SELECT tagname, ts as timestamp, f_value_numeric, l_value_numeric
FROM (
SELECT tagname, TIME_SLICE(DATEADD(MILLISECOND, -1, timestamp), 5, 'MINUTE', 'END') AS ts, timestamp, value_numeric, FIRST_VALUE(value_numeric) OVER (PARTITION BY tagname, ts ORDER BY timestamp) AS f_value_numeric, LAST_VALUE(value_numeric) OVER (PARTITION BY tagname, ts ORDER BY timestamp) AS l_value_numeric 
FROM HOL_TIMESERIES.ANALYTICS.TS_TAG_READINGS  
WHERE timestamp > '2000-03-26 03:29:08' AND timestamp <= '2024-03-28 15:29:08' 
AND tagname = '/WITSML/NO 15/9-F-7/DEPTH' 
GROUP BY TIME_SLICE(DATEADD(MILLISECOND, -1, timestamp), 5, 'MINUTE', 'END'), timestamp, tagname, value_numeric
)
GROUP BY tagname, ts, f_value_numeric, l_value_numeric
ORDER BY tagname, ts
;

-- STDDEV
SELECT tagname, TIME_SLICE(DATEADD(MILLISECOND, -1, timestamp), 15, 'MINUTE', 'END') AS timestamp, NULL AS value, STDDEV(value_numeric) AS value_numeric 
FROM HOL_TIMESERIES.ANALYTICS.TS_TAG_READINGS 
WHERE timestamp > '2000-03-26 15:00:50' AND timestamp <= '2024-03-25 15:00:50' 
AND tagname = '/WITSML/NO 15/9-F-7/DEPTH' 
GROUP BY TIME_SLICE(DATEADD(MILLISECOND, -1, timestamp), 15, 'MINUTE', 'END'), tagname 
ORDER BY tagname, timestamp
;

select unique_id
from TIME_SERIES_DATA_GENERATOR.CORE.SYNTHETIC_DATA
group by unique_id;

select * from TIME_SERIES_DATA_GENERATOR.CORE.SYNTHETIC_DATA
where unique_id = 28
order by date_time;