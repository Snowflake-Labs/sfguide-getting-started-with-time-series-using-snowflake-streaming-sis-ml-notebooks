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

# Get current session
session = get_active_session()

# Set page config
st.set_page_config(page_title="Time Series - Aggregates", layout="wide")

# Setup session state variables
if "times_refreshed" not in st.session_state:
    st.session_state["times_refreshed"] = 0
if "selected_tag" not in st.session_state:
    st.session_state["selected_tag"] = []
if "start_date" not in st.session_state:
    st.session_state["start_date"] = datetime.datetime.now(datetime.timezone.utc) - timedelta(hours=1)
if "start_time" not in st.session_state:
    st.session_state["start_time"] = datetime.datetime.now(datetime.timezone.utc) - timedelta(hours=1)
if "end_date" not in st.session_state:
    st.session_state["end_date"] = datetime.datetime.now(datetime.timezone.utc) + timedelta(hours=1)
if "end_time" not in st.session_state:
    st.session_state["end_time"] = datetime.datetime.now(datetime.timezone.utc) + timedelta(hours=1)
if "sample" not in st.session_state:
    st.session_state["sample"] = 500

# Page title
st.title('Time Series - Statistics and Aggregates')

# Get a list of all tags
df_tags = session.table('TS_TAG_REFERENCE').select(F.col('TAGNAME')).sort(F.col('TAGNAME')).toPandas()

st.sidebar.markdown('## Tag Selection')
taglist = st.sidebar.multiselect('Select Tag Names', df_tags, default=st.session_state["selected_tag"])
filter = (*taglist, "", "")

# Handle the SQL tuple for taglist
if len(taglist) == 1:
    tag_tuple = f"('{taglist[0]}')"
elif taglist:
    tag_tuple = str(tuple(taglist))
else:
    tag_tuple = "('')"

# Fetch tag metadata for selected tags 
if taglist:
    query = """
    SELECT TAGNAME, TAGUNITS, TAGDATATYPE, TAGDESCRIPTION FROM TS_TAG_REFERENCE
    WHERE TAGNAME IN ({})
    """.format(", ".join(f"'{tag}'" for tag in taglist))
    df_tag_metadata = session.sql(query).toPandas()
else:
    df_tag_metadata = pd.DataFrame(columns=['TAGNAME', 'TAGUNITS', 'TAGDATATYPE', 'TAGDESCRIPTION'])

# Set time range
st.sidebar.markdown('## Time Selection (UTC)')
start_date = st.sidebar.date_input('Start Date', st.session_state["start_date"], datetime.date(1995, 1, 1), datetime.date(2030, 12, 31))
start_time = st.sidebar.time_input('Start Time', st.session_state["start_time"])
end_date = st.sidebar.date_input('End Date', st.session_state["end_date"], datetime.date(1995, 1, 1), datetime.date(2030, 12, 31))
end_time = st.sidebar.time_input('End Time', st.session_state["end_time"])

# Update session state
st.session_state["selected_tag"] = taglist
st.session_state["start_date"] = start_date
st.session_state["start_time"] = start_time
st.session_state["end_date"] = end_date
st.session_state["end_time"] = end_time

# Combine start and end date time components
start_ts = datetime.datetime.combine(start_date, start_time)
end_ts = datetime.datetime.combine(end_date, end_time)

# Stats queries
stat_queries = {
    'Average': "SELECT TAGNAME, TO_TIMESTAMP('{end_ts}') AS TIMESTAMP, AVG(VALUE_NUMERIC) AS VALUE FROM TS_TAG_READINGS WHERE TIMESTAMP >= '{start_ts}' AND TIMESTAMP < '{end_ts}' AND TAGNAME IN {tag_tuple} GROUP BY TAGNAME",
    'Count': "SELECT TAGNAME, TO_TIMESTAMP('{end_ts}') AS TIMESTAMP, COUNT(VALUE) AS VALUE FROM TS_TAG_READINGS WHERE TIMESTAMP >= '{start_ts}' AND TIMESTAMP < '{end_ts}' AND TAGNAME IN {tag_tuple} GROUP BY TAGNAME",
    'Count Distinct': "SELECT TAGNAME, TO_TIMESTAMP('{end_ts}') AS TIMESTAMP, COUNT(DISTINCT VALUE) AS VALUE FROM TS_TAG_READINGS WHERE TIMESTAMP >= '{start_ts}' AND TIMESTAMP < '{end_ts}' AND TAGNAME IN {tag_tuple} GROUP BY TAGNAME",
    'Standard Deviation': "SELECT TAGNAME, TO_TIMESTAMP('{end_ts}') AS TIMESTAMP, STDDEV(VALUE_NUMERIC) AS VALUE FROM TS_TAG_READINGS WHERE TIMESTAMP >= '{start_ts}' AND TIMESTAMP < '{end_ts}' AND TAGNAME IN {tag_tuple} GROUP BY TAGNAME",
    'Variance': "SELECT TAGNAME, TO_TIMESTAMP('{end_ts}') AS TIMESTAMP, VARIANCE(VALUE_NUMERIC) AS VALUE FROM TS_TAG_READINGS WHERE TIMESTAMP >= '{start_ts}' AND TIMESTAMP < '{end_ts}' AND TAGNAME IN {tag_tuple} GROUP BY TAGNAME",
    '50th Percentile': "SELECT TAGNAME, TO_TIMESTAMP('{end_ts}') AS TIMESTAMP, APPROX_PERCENTILE(VALUE_NUMERIC, 0.5) AS VALUE FROM TS_TAG_READINGS WHERE TIMESTAMP >= '{start_ts}' AND TIMESTAMP < '{end_ts}' AND TAGNAME IN {tag_tuple} GROUP BY TAGNAME",
    '95th Percentile': "SELECT TAGNAME, TO_TIMESTAMP('{end_ts}') AS TIMESTAMP, APPROX_PERCENTILE(VALUE_NUMERIC, 0.95) AS VALUE FROM TS_TAG_READINGS WHERE TIMESTAMP >= '{start_ts}' AND TIMESTAMP < '{end_ts}' AND TAGNAME IN {tag_tuple} GROUP BY TAGNAME",
}

# Display all metrics in a container
if taglist:
    with st.container():
        st.subheader('Tag Metadata')
        st.dataframe(df_tag_metadata, hide_index=True, use_container_width=True)
        # Iterate over each tag and fetch metrics
        st.subheader(f"Statistical Metrics:")
        st.markdown(f"##### Timestamp: {end_ts}")
        for tag in taglist:
            
            
            metrics = []

            for name, query in stat_queries.items():
                # Format the query for the current tag
                formatted_query = query.format(start_ts=start_ts, end_ts=end_ts, tag_tuple=f"('{tag}')")
                try:
                    result = session.sql(formatted_query).collect()[0][2]
                except Exception as e:
                    continue

                if isinstance(result, float):
                    result = round(result, 2)
                metrics.append((name, result))

            # Display metrics for the current tag
            if len(metrics) > 0:
                st.markdown(f"###### {tag}:")
                columns = st.columns(len(metrics))
                for col, metric in zip(columns, metrics):
                    col.metric(label=metric[0], value=metric[1])
            else:
                st.write(f"No data for tag {tag}.")

if taglist:
    with st.expander("🔍 Supporting Detail", expanded=False):
        for name, query in stat_queries.items():
            formatted_query = query.format(start_ts=start_ts, 
                                        end_ts=end_ts,
                                        tag_tuple=tag_tuple)
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