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

# Get current session
session = get_active_session()

# Set page config
st.set_page_config(page_title="Time Series - Advanced", layout="wide")

# Setup session state variables
if "times_refreshed" not in st.session_state:
    st.session_state["times_refreshed"] = 0
if "refresh_mode" not in st.session_state:
    st.session_state["refresh_mode"] = False
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
    st.session_state["sample"] = 1000

# Page title
st.title('Time Series - Advanced')

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
    SELECT NAMESPACE, TAGNAME, TAGUNITS, TAGDATATYPE, TAGSOURCE FROM TS_TAG_REFERENCE
    WHERE TAGNAME IN ({})
    """.format(", ".join(f"'{tag}'" for tag in taglist))
    df_tag_metadata = session.sql(query).sort(F.col('TAGNAME')).toPandas()
else:
    df_tag_metadata = pd.DataFrame(columns=['NAMESPACE','TAGNAME', 'TAGUNITS', 'TAGDATATYPE', 'TAGSOURCE'])

# Update session state
st.session_state["selected_tag"] = taglist

# Set time range
st.sidebar.markdown('## Time Selection (UTC)')
start_date = st.sidebar.date_input('Start Date', st.session_state["start_date"], datetime.date(1995, 1, 1), datetime.date(2030, 12, 31))
start_time = st.sidebar.time_input('Start Time', st.session_state["start_time"])
end_date = st.sidebar.date_input('End Date', st.session_state["end_date"], datetime.date(1995, 1, 1), datetime.date(2030, 12, 31))
end_time = st.sidebar.time_input('End Time', st.session_state["end_time"])

# Combine start and end date time components
start_ts = datetime.datetime.combine(start_date, start_time)
end_ts = datetime.datetime.combine(end_date, end_time)

# Chart sampling
sample = st.sidebar.slider('Chart Sample', 100, 5000, st.session_state["sample"])

# Update session state
st.session_state["selected_tag"] = taglist
st.session_state["start_date"] = start_date
st.session_state["start_time"] = start_time
st.session_state["end_date"] = end_date
st.session_state["end_time"] = end_time
st.session_state["sample"] = sample
