# Import libraries
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import sum, col, when, max, lag
from snowflake.snowpark import Window
from datetime import timedelta
import altair as alt
import streamlit as st
import pandas as pd
import datetime

# Set page config
st.set_page_config(layout="wide")

# Get current session
# session = get_active_session()

# -- Query Type
detectorlist = ['Raw','Downsample', 'Binning']

today = datetime.datetime.now()
d = st.date_input("End Time", today, format="YYYY/MM/DD")
st.write('Data selected is:', d)

# st.title("Date range")
# # min_date = datetime.datetime(2020,1,1)
# # max_date = datetime.datetime(2022,1,1)
# # max_time = st.time_input("Pick a date", step=60)
# # min_time = st.time_input("Pick a date", step=60)

# st.sidebar.markdown('## Time Input')
# start_date = st.sidebar.selectbox('Start Time', st.date_input("Start Time", today, format="YYYY/MM/DD"))
# end_time = st.sidebar.selectbox('End Time', st.date_input("End Time", today, format="YYYY/MM/DD"))

# -- Select for high sample rate data
fs = 4096
maxband = 1200
high_fs = st.sidebar.checkbox('Full sample rate data')
if high_fs:
    fs = 16384
    maxband = 2000


# -- Create sidebar for plot controls
st.sidebar.markdown('## Set Plot Parameters')
dtboth = st.sidebar.slider('Time Range (seconds)', 0.1, 8.0, 1.0)  # min, max, default
dt = dtboth / 2.0

st.sidebar.markdown('#### Whitened and band-passed data')
whiten = st.sidebar.checkbox('Whiten?', value=True)
freqrange = st.sidebar.slider('Band-pass frequency range (Hz)', min_value=10, max_value=maxband, value=(30,400))


# -- Create sidebar for Q-transform controls
st.sidebar.markdown('#### Q-tranform plot')
vmax = st.sidebar.slider('Colorbar Max Energy', 10, 500, 25)  # min, max, default
qcenter = st.sidebar.slider('Q-value', 5, 120, 5)  # min, max, default
qrange = (int(qcenter*0.8), int(qcenter*1.2))
