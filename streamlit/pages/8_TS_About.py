# Import libraries
import snowflake.snowpark as snow
from snowflake.snowpark.context import get_active_session
import altair as alt
import streamlit as st
import pandas as pd
import datetime

# Use active session
session = get_active_session()

# Set page config
st.set_page_config(page_title="Time Series - Home", layout="wide")

# Make a dataframe of images for display
stg_name = "HOL_TIMESERIES.ANALYTICS.STAGE_TS_STREAMLIT"
session.sql(f"""ALTER STAGE {stg_name} REFRESH""").collect()
img_sql = f"""
SELECT
RELATIVE_PATH AS FILE_NAME,
GET_PRESIGNED_URL(@{stg_name},RELATIVE_PATH) AS IMG_URL
FROM DIRECTORY(@{stg_name}) WHERE RELATIVE_PATH ilike '%snowflakelogo.png'
"""
img_df = session.sql(img_sql).collect()
for img in img_df:
    st.image(image=img["IMG_URL"], width=300)

# Page Title
st.title('Snowflake Package Details')

st.markdown("snowflake.snowpark: " + snow.__version__)
st.markdown("streamlit: " + st.__version__)
st.markdown("pandas: " + pd.__version__)
st.markdown("altair: " + alt.__version__)