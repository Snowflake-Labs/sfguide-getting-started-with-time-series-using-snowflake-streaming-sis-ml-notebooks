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
import json

# Set page config
st.set_page_config(layout="wide")
st.title('Time Series - Statistical Queries')

# Get current session
session = get_active_session()

# get a list of all tags
df_tags = session.table('TS_TAG_REFERENCE').select(F.col('TAGNAME')).toPandas()

# Query Type
query_profile = ['Average','Count', 'Count Distinct']

st.sidebar.markdown('## Tag Selection')
taglist = st.sidebar.multiselect('Select Tag Names', df_tags)
filter = (*taglist, "", "")

# Fetch tag metadata for selected tags 
if taglist:
    query = """
    SELECT TAGNAME, TAGUNITS, TAGDATATYPE FROM TS_TAG_REFERENCE
    WHERE TAGNAME IN ({})
    """.format(", ".join(f"'{tag}'" for tag in taglist))
    df_tag_metadata = session.sql(query).toPandas()
else:
    df_tag_metadata = pd.DataFrame(columns=['TAGNAME', 'TAGUNITS', 'TAGDATATYPE'])

st.dataframe(df_tag_metadata, hide_index=True, use_container_width=True)

# Set time range
st.sidebar.markdown('## Time Selection (UTC)')
start_date = st.sidebar.date_input('Start Date', datetime.datetime.now(datetime.timezone.utc) - timedelta(hours=8), datetime.date(2024, 1, 1), datetime.date(2030, 12, 31))
end_date = st.sidebar.date_input('End Date', datetime.datetime.now(datetime.timezone.utc), datetime.date(2024, 1, 1), datetime.date(2030, 12, 31))
# change logic to use modulo to ensure hour is between 0 and 23  
current_hour = int(datetime.datetime.now(datetime.timezone.utc).strftime("%H"))
current_minute = int(datetime.datetime.now(datetime.timezone.utc).strftime("%M"))
start_hour = (current_hour - 8) % 24 

start_time, end_time = st.sidebar.slider(
    "Time range",
    value=(
        time(start_hour, current_minute),
        time(current_hour, current_minute)
    )
)

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

# TS dataframe with query variables
df_data = session.sql(
    query_str \
        .replace("{start_ts}", str(start_ts)) \
        .replace("{end_ts}", str(end_ts)) \
        .replace("{taglist}", str(tuple(filter)))
        )



# Define stats queries
stat_queries = {
    'Average': "SELECT AVG(VALUE_NUMERIC) AS Average FROM TS_TAG_READINGS WHERE TIMESTAMP >= '{start_ts}' AND TIMESTAMP < '{end_ts}' AND TAGNAME = '{tag}'",
    'Count': "SELECT COUNT(VALUE) AS Count FROM TS_TAG_READINGS WHERE TIMESTAMP >= '{start_ts}' AND TIMESTAMP < '{end_ts}' AND TAGNAME = '{tag}'",
    'Count Distinct': "SELECT COUNT(DISTINCT VALUE) AS Count_Distinct FROM TS_TAG_READINGS WHERE TIMESTAMP >= '{start_ts}' AND TIMESTAMP < '{end_ts}' AND TAGNAME = '{tag}'",
    'Standard Deviation': "SELECT STDDEV(VALUE_NUMERIC) AS VALUE FROM TS_TAG_READINGS WHERE TIMESTAMP >= '{start_ts}' AND TIMESTAMP < '{end_ts}' AND TAGNAME = '{tag}'",
    'Variance': "SELECT VARIANCE(VALUE_NUMERIC) AS VALUE FROM TS_TAG_READINGS WHERE TIMESTAMP >= '{start_ts}' AND TIMESTAMP < '{end_ts}' AND TAGNAME = '{tag}'"
}

# Select default tag
selected_tag = taglist[0] if taglist else '/IOT/SENSOR/100' 


# Create chart plot
with st.container():
    st.subheader('Tag Data')
    alt_chart_1 = alt.Chart(df_data.to_pandas()).mark_line().encode(x="TIMESTAMP", y="VALUE", color="TAGNAME").interactive()
    st.altair_chart(alt_chart_1, use_container_width=True)
    # fig = px.line(df_data, x='TIMESTAMP', y='VALUE_NUMERIC', color='TAGNAME')
    # st.plotly_chart(fig, use_container_width=True, render='svg')

    st.dataframe(df_data.collect(), hide_index=True, use_container_width=True)

# Display all metrics in a container
with st.container():
    st.write("Statistical Metrics:")
    for name, query in stat_queries.items():
        result = session.sql(query.format(start_ts=start_ts, end_ts=end_ts, tag=selected_tag)).collect()[0][0]
        st.metric(label=name, value=result)

with st.expander("Supporting Detail", expanded=False):
    st.write(taglist)
    st.write(str(tuple(filter)))
    st.write(query_str)
    st.write(start_ts)
    st.write(end_ts)