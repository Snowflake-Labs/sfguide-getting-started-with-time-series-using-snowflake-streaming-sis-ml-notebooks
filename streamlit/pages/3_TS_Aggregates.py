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
st.title('Time Series - Aggregates')

# Get current session
session = get_active_session()

# get a list of all tags
df_tags = session.table('TS_TAG_REFERENCE').select(F.col('TAGNAME')).toPandas()

# Query Type
query_profile = ['Average','Count', 'Count Distinct']

st.sidebar.markdown('## Tag Selection')
taglist = st.sidebar.selectbox('Select Tag Names', df_tags)

# Fetch tag metadata for selected tags 
if taglist:
    query = """
    SELECT TAGNAME, TAGUNITS, TAGDATATYPE FROM TS_TAG_REFERENCE
    WHERE TAGNAME IN ({})
    """.format(", ".join(f"'{tag}'" for tag in taglist))
    df_tag_metadata = session.sql(query).toPandas()
else:
    df_tag_metadata = pd.DataFrame(columns=['TAGNAME', 'TAGUNITS', 'TAGDATATYPE'])

# Set time range
st.sidebar.markdown('## Time Selection (UTC)')
start_date = st.sidebar.date_input('Start Date', datetime.datetime.now(datetime.timezone.utc) - timedelta(hours=8), datetime.date(1995, 1, 1), datetime.date(2030, 12, 31))
end_date = st.sidebar.date_input('End Date', datetime.datetime.now(datetime.timezone.utc), datetime.date(1995, 1, 1), datetime.date(2030, 12, 31))
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

# Combine start and end date time components
start_ts = datetime.datetime.combine(start_date, start_time)
end_ts = datetime.datetime.combine(end_date, end_time)

# Stats queries
stat_queries = {
    'Average': "SELECT AVG(VALUE_NUMERIC) AS Average FROM TS_TAG_READINGS WHERE TIMESTAMP >= '{start_ts}' AND TIMESTAMP < '{end_ts}' AND TAGNAME = '{tag}'",
    'Count': "SELECT COUNT(VALUE) AS Count FROM TS_TAG_READINGS WHERE TIMESTAMP >= '{start_ts}' AND TIMESTAMP < '{end_ts}' AND TAGNAME = '{tag}'",
    'Count Distinct': "SELECT COUNT(DISTINCT VALUE) AS Count_Distinct FROM TS_TAG_READINGS WHERE TIMESTAMP >= '{start_ts}' AND TIMESTAMP < '{end_ts}' AND TAGNAME = '{tag}'",
    'Standard Deviation': "SELECT STDDEV(VALUE_NUMERIC) AS VALUE FROM TS_TAG_READINGS WHERE TIMESTAMP >= '{start_ts}' AND TIMESTAMP < '{end_ts}' AND TAGNAME = '{tag}'",
    'Variance': "SELECT VARIANCE(VALUE_NUMERIC) AS VALUE FROM TS_TAG_READINGS WHERE TIMESTAMP >= '{start_ts}' AND TIMESTAMP < '{end_ts}' AND TAGNAME = '{tag}'"
}

# Display all metrics in a container
with st.container():
    st.write("Statistical Metrics:")
    columns = st.columns(len(stat_queries))  
    metrics = []
    
    for name, query in stat_queries.items():
        formatted_query = query.format(start_ts=start_ts, 
                                       end_ts=end_ts,
                                       tag=taglist)
        result = session.sql(formatted_query).collect()[0][0]
        if isinstance(result, float):
            result = round(result, 2)
        metrics.append((name, result))
    
    for col, metric in zip(columns, metrics):
        col.metric(label=metric[0], value=metric[1])

with st.expander("Supporting Detail", expanded=False):
    for name, query in stat_queries.items():
        formatted_query = query.format(start_ts=start_ts, 
                                       end_ts=end_ts,
                                       tag=taglist)
        st.subheader(name + " Query:")
        st.code(formatted_query, language="sql")