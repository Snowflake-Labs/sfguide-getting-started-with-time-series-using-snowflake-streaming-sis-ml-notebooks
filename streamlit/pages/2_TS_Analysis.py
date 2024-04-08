# Import libraries
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.types import *
from snowflake.snowpark import functions as F
from snowflake.snowpark import Window
from datetime import time, timedelta
import altair as alt
import plotly.express as px
import streamlit as st
import pandas as pd
import datetime

# Set page config
st.set_page_config(layout="wide")

# Get current session
session = get_active_session()

# get a list of all tags
df_tags = session.table('TS_TAG_REFERENCE').select(F.col('TAGNAME')).toPandas()

# Query Type
query_profile = ['Raw','Downsample', 'Binning']

st.sidebar.markdown('## Tag Selection')
taglist = st.sidebar.multiselect('Select Tag Names', df_tags)

st.sidebar.markdown('## Time Selection')
start_date = st.sidebar.date_input('Start Date', datetime.datetime.now() - timedelta(days=7), datetime.date(2000, 1, 1), datetime.date(2030, 12, 31))
end_date = st.sidebar.date_input('End Date', datetime.datetime.now(), datetime.date(2000, 1, 1), datetime.date(2030, 12, 31))
start_time, end_time = st.sidebar.slider("Time range",value=(time(00, 00), time(00, 00)))

# get the timeseries history
# df_raw = session.table('TS_TAG_READINGS') \
#     .select( \
#         F.col('TAGNAME'), \
#         F.col('TIMESTAMP'), \
#         F.col('VALUE_NUMERIC')) \
#     .filter( \
#         (F.col('TIMESTAMP') >= F.lit(start_date)) & \
#         (F.col('TIMESTAMP') < F.lit(end_date)) & \
#         (F.col('TAGNAME')).isin(taglist))

sql_str = '''
SELECT data.tagname, lttb.timestamp::varchar::timestamp_ntz AS timestamp, NULL AS value, lttb.value_numeric
FROM (
SELECT tagname, timestamp, value_numeric
FROM TS_TAG_READINGS
WHERE timestamp >= DATE '{start_date}' AND timestamp < DATE '{end_date}'
AND tagname IN {taglist}
) AS data
CROSS JOIN TABLE(function_ts_lttb(date_part(epoch_nanosecond, data.timestamp), data.value_numeric, 500) OVER (PARTITION BY data.tagname ORDER BY data.timestamp)) AS lttb
ORDER BY tagname, timestamp'''

st.write(taglist)
filter = (*taglist, "", "")
st.write(str(tuple(filter)))
st.write(sql_str)

df_raw = session.sql(
    sql_str \
        .replace("{start_date}", str(start_date)) \
        .replace("{end_date}", str(end_date)) \
        .replace("{taglist}", str(tuple(filter))))

# add the line charts
with st.container():
    st.subheader('Tag Data')
    alt_chart_1 = alt.Chart(df_raw.to_pandas()).mark_line().encode(x="TIMESTAMP",y="VALUE_NUMERIC")
    st.altair_chart(alt_chart_1, use_container_width=True)
    # fig = px.line(df_raw, x='TIMESTAMP', y='VALUE_NUMERIC', color='TAGNAME')
    # st.plotly_chart(fig, use_container_width=True, render='svg')