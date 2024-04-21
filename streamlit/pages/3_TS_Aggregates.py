# Import libraries
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.types import *
from snowflake.snowpark import functions as F
from snowflake.snowpark import Window
from datetime import time, timedelta
import time
import altair as alt
import plotly.express as px
import streamlit as st
import pandas as pd
import datetime
import json

# Set page config
st.set_page_config(layout="wide")
st.title('Time Series - Statistics and Aggregates')

# Get current session
session = get_active_session()

# Setup session state variables
if "times_refreshed" not in st.session_state:
    st.session_state["times_refreshed"] = 0

# get a list of all tags
df_tags = session.table('TS_TAG_REFERENCE').select(F.col('TAGNAME')).sort(F.col('TAGNAME')).toPandas()

# Query Type
query_profile = ['Average','Count', 'Count Distinct']

st.sidebar.markdown('## Tag Selection')
taglist = st.sidebar.selectbox('Select Tag Names', df_tags)

# Fetch tag metadata for selected tags 
if taglist:
    query = """
    SELECT TAGNAME, TAGUNITS, TAGDATATYPE, TAGDESCRIPTION FROM TS_TAG_REFERENCE
    WHERE TAGNAME = '{tags}'
    """.format(tags=taglist)
    df_tag_metadata = session.sql(query).toPandas()
else:
    df_tag_metadata = pd.DataFrame(columns=['TAGNAME', 'TAGUNITS', 'TAGDATATYPE', 'TAGDESCRIPTION'])

# Set time range
st.sidebar.markdown('## Time Selection (UTC)')
start_date = st.sidebar.date_input('Start Date', datetime.datetime.now(datetime.timezone.utc) - timedelta(hours=1), datetime.date(1995, 1, 1), datetime.date(2030, 12, 31))
start_time = st.sidebar.time_input('Start Time', datetime.datetime.now(datetime.timezone.utc) - timedelta(hours=1))
end_date = st.sidebar.date_input('End Date', datetime.datetime.now(datetime.timezone.utc) + timedelta(hours=1), datetime.date(1995, 1, 1), datetime.date(2030, 12, 31))
end_time = st.sidebar.time_input('End Time', datetime.datetime.now(datetime.timezone.utc) + timedelta(hours=1))

# change logic to use modulo to ensure hour is between 0 and 23  
# current_hour = int(datetime.datetime.now(datetime.timezone.utc).strftime("%H"))
# current_minute = int(datetime.datetime.now(datetime.timezone.utc).strftime("%M"))
# start_hour = (current_hour - 8) % 24 

# start_time, end_time = st.sidebar.slider(
#     "Time range",
#     value=(
#         datetime.time(start_hour, current_minute),
#         datetime.time(current_hour, current_minute)
#     )
# )

# Combine start and end date time components
start_ts = datetime.datetime.combine(start_date, start_time)
end_ts = datetime.datetime.combine(end_date, end_time)

# Stats queries
stat_queries = {
    'Average': "SELECT TAGNAME, TO_TIMESTAMP('{end_ts}') AS TIMESTAMP, AVG(VALUE_NUMERIC) AS VALUE FROM TS_TAG_READINGS WHERE TIMESTAMP >= '{start_ts}' AND TIMESTAMP < '{end_ts}' AND TAGNAME = '{tag}' GROUP BY TAGNAME",
    'Count': "SELECT TAGNAME, TO_TIMESTAMP('{end_ts}') AS TIMESTAMP, COUNT(VALUE) AS VALUE FROM TS_TAG_READINGS WHERE TIMESTAMP >= '{start_ts}' AND TIMESTAMP < '{end_ts}' AND TAGNAME = '{tag}' GROUP BY TAGNAME",
    'Count Distinct': "SELECT TAGNAME, TO_TIMESTAMP('{end_ts}') AS TIMESTAMP, COUNT(DISTINCT VALUE) AS VALUE FROM TS_TAG_READINGS WHERE TIMESTAMP >= '{start_ts}' AND TIMESTAMP < '{end_ts}' AND TAGNAME = '{tag}' GROUP BY TAGNAME",
    'Standard Deviation': "SELECT TAGNAME, TO_TIMESTAMP('{end_ts}') AS TIMESTAMP, STDDEV(VALUE_NUMERIC) AS VALUE FROM TS_TAG_READINGS WHERE TIMESTAMP >= '{start_ts}' AND TIMESTAMP < '{end_ts}' AND TAGNAME = '{tag}' GROUP BY TAGNAME",
    'Variance': "SELECT TAGNAME, TO_TIMESTAMP('{end_ts}') AS TIMESTAMP, VARIANCE(VALUE_NUMERIC) AS VALUE FROM TS_TAG_READINGS WHERE TIMESTAMP >= '{start_ts}' AND TIMESTAMP < '{end_ts}' AND TAGNAME = '{tag}' GROUP BY TAGNAME"
}

# Display all metrics in a container
if taglist:
    with st.container():
        st.subheader('Tag Metadata')
        st.dataframe(df_tag_metadata, hide_index=True, use_container_width=True)

        metrics = []
        
        for name, query in stat_queries.items():
            formatted_query = query.format(start_ts=start_ts, 
                                        end_ts=end_ts,
                                        tag=taglist)
            try:
                result = session.sql(formatted_query).collect()[0][2]
            except:
                continue

            if isinstance(result, float):
                result = round(result, 2)
            metrics.append((name, result))

        if len(metrics) > 0:
            st.subheader("Statistical Metrics:")
            columns = st.columns(len(stat_queries))  

            for col, metric in zip(columns, metrics):
                col.metric(label=metric[0], value=metric[1])
        else:
            st.write("❄️ No data for selection.")

if taglist:
    with st.expander("🔍 Supporting Detail", expanded=False):
        for name, query in stat_queries.items():
            formatted_query = query.format(start_ts=start_ts, 
                                        end_ts=end_ts,
                                        tag=taglist)
            st.subheader(name + " Query:")
            st.code(formatted_query, language="sql")

if taglist:
    with st.expander("♻️ Refresh Mode", expanded=False):
        refresh_section = st.columns((1, 1, 6))
        st.session_state["times_refreshed"] += 1
        with refresh_section[1]:
            st.info(f"**Times Refreshed** : {st.session_state['times_refreshed']} ")
        with refresh_section[0]:
            refresh_mode = st.radio(
                "Refresh Automatically?", options=["Yes", "No"], index=1, horizontal=True
            )

        if refresh_mode == "Yes":
            refresh_section[2].success("Data will refresh every minute")
            progress_b = refresh_section[2].progress(0)
            for percent_complete in range(100):
                time.sleep(0.6)
                progress_b.progress(percent_complete + 1)

            st.experimental_rerun()

        if refresh_mode == "No":
            refresh = st.button("Refresh")
            if refresh:
                st.experimental_rerun()