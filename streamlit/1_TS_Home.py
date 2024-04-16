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
st.title('Getting Started with Time Series')

# Use active session
session = get_active_session()