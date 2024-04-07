USE ROLE ROLE_HOL_TIMESERIES;
USE HOL_TIMESERIES.ANALYTICS;

-- LTTB Downsampling Table Function
CREATE OR REPLACE FUNCTION HOL_TIMESERIES.ANALYTICS.FUNCTION_TS_LTTB (
    TIMESTAMP NUMBER,
    VALUE_NUMERIC FLOAT,
    SIZE NUMBER
) 
RETURNS TABLE (
    TIMESTAMP NUMBER,
    VALUE_NUMERIC FLOAT
)
LANGUAGE PYTHON
RUNTIME_VERSION = 3.11
PACKAGES = ('pandas', 'plotly-resampler', 'setuptools')
HANDLER = 'lttb_run'
AS $$
from _snowflake import vectorized
import pandas as pd
from plotly_resampler.aggregation.algorithms.lttb_py import LTTB_core_py

class lttb_run:
    @vectorized(input=pd.DataFrame)

    def end_partition(self, df):
        if df.SIZE.max() >= len(df.index):
            return df[['TIMESTAMP','VALUE_NUMERIC']]
        else:
            idx = LTTB_core_py.downsample(
                df.TIMESTAMP.to_numpy(),
                df.VALUE_NUMERIC.to_numpy(),
                n_out=df.SIZE.max()
            )
            return df[['TIMESTAMP','VALUE_NUMERIC']].iloc[idx]
$$;


-- LTTB
SELECT data.tagname, lttb.timestamp::varchar::timestamp_ntz AS timestamp, NULL AS value, lttb.value_numeric 
FROM (
SELECT tagname, TIME_SLICE(DATEADD(MILLISECOND, -1, timestamp), 1, 'SECOND', 'END') AS timestamp, APPROX_PERCENTILE(value_numeric, 0.5) AS value_numeric 
FROM HOL_TIMESERIES.ANALYTICS.TS_TAG_READINGS
WHERE timestamp > '2000-03-26 02:50:21' AND timestamp <= '2024-03-26 14:50:21' 
AND tagname IN ('/WITSML/NO 15/9-F-7/DEPTH') 
GROUP BY tagname, TIME_SLICE(DATEADD(MILLISECOND, -1, timestamp), 1, 'SECOND', 'END')
) AS data 
CROSS JOIN TABLE(HOL_TIMESERIES.ANALYTICS.function_ts_lttb(date_part(epoch_nanosecond, data.timestamp), data.value_numeric, 500) OVER (PARTITION BY data.tagname ORDER BY data.timestamp)) AS lttb
ORDER BY tagname, timestamp
;
