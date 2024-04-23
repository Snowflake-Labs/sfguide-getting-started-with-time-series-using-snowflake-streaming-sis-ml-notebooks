# Import libraries
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import sum, col, when, max, lag
from snowflake.snowpark import Window
from datetime import timedelta
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
st.title('Getting Started with Time Series')

# Container for the descriptions
with st.container():
    st.write("## Overview")
    st.markdown("""
        Welcome to the **Time Series Analysis Dashboard**. This app provides comprehensive analysis tools for time series data, focusing on statistical metrics and data visualization. Below is an overview of the different sections available within this dashboard:
    """)
    

    # - **View Data**: Navigate through the raw time series data and observe key metrics in a graph and table providing a detailed statistical observation of the source data.
    # Use columns to create a two-column layout for the page descriptions
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("### Raw Data Exploration")
        st.markdown("""   
            - **View Raw Data**: Directly access and explore raw time series data.
            - **Interactive Visualization**: Utilize interactive charts to visualize data across various time spans.
            - **Data Filtering**: Apply filters to view specific subsets of data based on tags and time ranges.
        """)
        
        st.write("### Binning")
        st.markdown("""
            - **Data Aggregation**: Aggregate data points into bins to reduce time series data complexity and noise.
            - **Enhanced Visualization**: View and analyze binned data to identify broader trends over adjustable time intervals.
        """)

    with col2:
        st.write("### Aggregates")
        st.markdown("""
            - **Data Aggregation**: Perform aggregation queries such as averages, counts, and distinct counts.
            - **Statistical Analysis**: Conduct detailed statistical analysis to understand data distributions, variance, and more.
        """)

        
        st.write("### About")
        st.markdown("""
            - **About Section**: Providing the details on the versions used on the tools for this application.
        """)
        
    st.write("## Getting Started")
    st.markdown("""
        To begin, navigate to the section of interest using the sidebar on the left. Each section is designed to help you perform specific types of analysis and extract insights from your time series data efficiently.
    """)