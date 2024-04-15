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

# Set time range
st.sidebar.markdown('## Time Selection (UTC)')
start_date = st.sidebar.date_input('Start Date', datetime.datetime.now(datetime.timezone.utc) - timedelta(hours=8), datetime.date(2024, 1, 1), datetime.date(2030, 12, 31))
end_date = st.sidebar.date_input('End Date', datetime.datetime.now(datetime.timezone.utc), datetime.date(2024, 1, 1), datetime.date(2030, 12, 31))
start_time, end_time = st.sidebar.slider("Time range",value=(time(int(datetime.datetime.now(datetime.timezone.utc).strftime("%H"))-8, int(datetime.datetime.now(datetime.timezone.utc).strftime("%M"))), time(int(datetime.datetime.now(datetime.timezone.utc).strftime("%H")), int(datetime.datetime.now(datetime.timezone.utc).strftime("%M")))))

# Chart sampling
sample = st.sidebar.slider('Chart Sample', 100, 1000, 500)

# Combine start and end date time components
start_ts = datetime.datetime.combine(start_date, start_time)
end_ts = datetime.datetime.combine(end_date, end_time)

# TS Query Builder
query_str = '''
SELECT DATA.TAGNAME, LTTB.TIMESTAMP::VARCHAR::TIMESTAMP_NTZ AS TIMESTAMP, LTTB.VALUE::FLOAT AS VALUE
FROM (
SELECT TAGNAME, TIMESTAMP, VALUE_NUMERIC AS VALUE
FROM TS_TAG_READINGS
WHERE TIMESTAMP >= TIMESTAMP '{start_ts}'
AND TIMESTAMP < TIMESTAMP '{end_ts}'
AND TAGNAME IN {taglist}
) AS DATA
CROSS JOIN TABLE(FUNCTION_TS_LTTB(DATE_PART(EPOCH_NANOSECOND, DATA.TIMESTAMP), DATA.VALUE, 100) OVER (PARTITION BY DATA.TAGNAME ORDER BY DATA.TIMESTAMP)) AS LTTB
ORDER BY TAGNAME, TIMESTAMP'''

st.write(taglist)
filter = (*taglist, "", "")
st.write(str(tuple(filter)))
st.write(query_str)

# TS dataframe with query variables
df_data = session.sql(
    query_str \
        .replace("{start_ts}", str(start_ts)) \
        .replace("{end_ts}", str(end_ts)) \
        .replace("{taglist}", str(tuple(filter))))

# Create chart plot
with st.container():
    st.subheader('Tag Data')
    alt_chart_1 = alt.Chart(df_data.to_pandas()).mark_line().encode(x="TIMESTAMP",y="VALUE")
    st.altair_chart(alt_chart_1, use_container_width=True)
    # fig = px.line(df_data, x='TIMESTAMP', y='VALUE_NUMERIC', color='TAGNAME')
    # st.plotly_chart(fig, use_container_width=True, render='svg')

    st.table(df_data.collect())