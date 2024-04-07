# Import libraries
import snowflake.snowpark as snow
import altair as alt
import streamlit as st
import pandas as pd
import datetime

# Set page config
st.set_page_config(layout="wide")

st.markdown("snowflake.snowpark: " + snow.__version__)
st.markdown("streamlit: " + st.__version__)
st.markdown("pandas: " + pd.__version__)
st.markdown("altair: " + alt.__version__)
