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

# To be used in the session
df_tags = session.table('TS_TAG_REFERENCE').select(F.col('TAGNAME')).toPandas()

# Query Type
query_profile = ['Raw','Downsample', 'Binning']

st.sidebar.markdown('## Tag Selection')
taglist = st.sidebar.multiselect('Select Tag Names', df_tags)

# TODO: Include timestamp slider into time delta select and default last 1 hour
st.sidebar.markdown('## Time Selection')
start_date = st.sidebar.date_input('Start Date', datetime.datetime.now() - timedelta(days=7), datetime.date(2000, 1, 1), datetime.date(2030, 12, 31))
end_date = st.sidebar.date_input('End Date', datetime.datetime.now(), datetime.date(2000, 1, 1), datetime.date(2030, 12, 31))
start_time, end_time = st.sidebar.slider("Time range",value=(time(00, 00), time(00, 00)))

# TS queries
