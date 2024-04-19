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
st.title('Time Series - Binning')

# Get current session
session = get_active_session()


# get a list of all tags
df_tags = session.table('TS_TAG_REFERENCE').select(F.col('TAGNAME')).toPandas()

# Query Type
query_profile = ['Raw','Downsample', 'Binning']

st.sidebar.markdown('## Tag Selection')
taglist = st.sidebar.multiselect('Select Tag Names', df_tags)
filter = (*taglist, "", "")


if len(taglist) == 1:
    tag_tuple = f"('{taglist[0]}')"
elif taglist:
    tag_tuple = str(tuple(taglist))
else:
    tag_tuple = "('')"   

# Fetch tag metadata for selected tags 
if taglist:
    query = """
    SELECT TAGNAME, TAGUNITS, TAGDATATYPE FROM TS_TAG_REFERENCE
    WHERE TAGNAME IN ({})
    """.format(", ".join(f"'{tag}'" for tag in taglist))
    df_tag_metadata = session.sql(query).toPandas()
else:
    df_tag_metadata = pd.DataFrame(columns=['TAGNAME', 'TAGUNITS', 'TAGDATATYPE'])

st.dataframe(df_tag_metadata, hide_index=True, use_container_width=True)

# Set time range
st.sidebar.markdown('## Time Selection (UTC)')
start_date = st.sidebar.date_input('Start Date', datetime.datetime.now(datetime.timezone.utc) - timedelta(hours=8), datetime.date(1995, 1, 1), datetime.date(2030, 12, 31))
end_date = st.sidebar.date_input('End Date', datetime.datetime.now(datetime.timezone.utc), datetime.date(1995, 1, 1), datetime.date(2030, 12, 31))
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
# Chart sampling
sample = st.sidebar.slider('Chart Sample', 100, 1000, 500)

# Combine start and end date time components
start_ts = datetime.datetime.combine(start_date, start_time)
end_ts = datetime.datetime.combine(end_date, end_time)

drop_down = ['Average', 'Count', 'Count Distinct', 'Standard Deviation', 'Variance']

agg_options = {
    'Average': "AVG(VALUE_NUMERIC)",
    'Count': "COUNT(VALUE)",
    'Count Distinct': "COUNT(DISTINCT VALUE)",
    'Standard Deviation': "STDDEV(VALUE_NUMERIC)",
    'Variance': "VARIANCE(VALUE_NUMERIC)"
}

# BINNING - Configuration
st.sidebar.markdown('## Binning Configuration')
selected_agg = st.sidebar.selectbox('Select Aggregation Method', list(agg_options.keys()))
bin_range = st.sidebar.number_input('Enter Bin Range', min_value=1, value=10)  
interval_unit = st.sidebar.selectbox('Select Interval Unit', ['seconds', 'minutes', 'hours', 'days', 'weeks'])
label_position = st.sidebar.radio('Label Position', ['Start', 'End'], index=1, horizontal=True) 



# TS Query Builder
# query_str = f'''
# SELECT DATA.TAGNAME, LTTB.TIMESTAMP::VARCHAR::TIMESTAMP_NTZ AS TIMESTAMP, LTTB.VALUE::FLOAT AS VALUE
# FROM (
#     SELECT TAGNAME, TIME_SLICE(DATEADD(MILLISECOND, -1, TIMESTAMP), {bin_range}, '{interval_unit}', '{label_position}') AS TIMESTAMP, {agg_options[selected_agg]} AS VALUE
#     FROM TS_TAG_READINGS
#     WHERE TIMESTAMP >=  '{start_ts}' AND TIMESTAMP < '{end_ts}' AND TAGNAME IN {tag_tuple}
#     GROUP BY TAGNAME, TIME_SLICE(DATEADD(MILLISECOND, -1, TIMESTAMP), {bin_range}, '{interval_unit}', '{label_position}')
#     ORDER BY TAGNAME, TIMESTAMP
# ) AS DATA
# CROSS JOIN TABLE(FUNCTION_TS_LTTB(DATE_PART(EPOCH_NANOSECOND, DATA.TIMESTAMP), DATA.VALUE, 100) OVER (PARTITION BY DATA.TAGNAME ORDER BY DATA.TIMESTAMP)) AS LTTB
# ORDER BY TAGNAME, TIMESTAMP
# '''

# Determine if we need the LTTB transformation
needs_transformation = selected_agg not in ['Count', 'Count Distinct']

# Construct the main SQL query part
sql_select = f"""
    SELECT TAGNAME, TIME_SLICE(DATEADD(MILLISECOND, -1, TIMESTAMP), {bin_range}, '{interval_unit}', '{label_position}') AS TIMESTAMP, {agg_options[selected_agg]} AS VALUE
    FROM TS_TAG_READINGS
    WHERE TIMESTAMP >= '{start_ts}' AND TIMESTAMP < '{end_ts}' AND TAGNAME IN {tag_tuple}
    GROUP BY TAGNAME, TIME_SLICE(DATEADD(MILLISECOND, -1, TIMESTAMP), {bin_range}, '{interval_unit}', '{label_position}')
"""

# Apply LTTB if needed
if needs_transformation:
    query_str = f'''
        SELECT DATA.TAGNAME, LTTB.TIMESTAMP::VARCHAR::TIMESTAMP_NTZ AS TIMESTAMP, LTTB.VALUE::FLOAT AS VALUE
        FROM ({sql_select}) AS DATA
        CROSS JOIN TABLE(FUNCTION_TS_LTTB(DATE_PART(EPOCH_NANOSECOND, DATA.TIMESTAMP), DATA.VALUE, 100) OVER (PARTITION BY DATA.TAGNAME ORDER BY DATA.TIMESTAMP)) AS LTTB
        ORDER BY TAGNAME, TIMESTAMP
    '''
else:
    query_str = f'''
        {sql_select}
        ORDER BY TAGNAME, TIMESTAMP
    '''


# Generate a SQL-compatible tuple from the tag list
tag_tuple = str(tuple(taglist)) if len(taglist) > 1 else f"('{taglist[0]}')" if taglist else "()"


# TS dataframe with query variables
df_data = session.sql(
    query_str \
        .replace("{start_ts}", str(start_ts)) \
        .replace("{end_ts}", str(end_ts)) \
        .replace("{taglist}", str(tuple(filter)))
        )

# Create chart plot
with st.container():
    st.subheader('Tag Data')
    st.write(f"Aggregation: {selected_agg}")
    alt_chart_1 = alt.Chart(df_data.to_pandas()).mark_line().encode(x="TIMESTAMP", y="VALUE", color="TAGNAME").interactive()
    st.altair_chart(alt_chart_1, use_container_width=True)
    # fig = px.line(df_data, x='TIMESTAMP', y='VALUE_NUMERIC', color='TAGNAME')
    # st.plotly_chart(fig, use_container_width=True, render='svg')
    
    # st.dataframe(df_data.collect(), hide_index=True, use_container_width=True) #REMOVED OLD TABLE HERE
    #st.table(df_data.collect())

 # Execute and display results
if taglist:
    df_binned_data = session.sql(query_str)
    with st.container():
        st.subheader('Binned Data')
        st.write(f"Aggregation: {selected_agg}")
        st.dataframe(df_binned_data.collect(), hide_index=True, use_container_width=True)
else:
    st.write("Select one or more tags to view binned data.")

with st.expander("Supporting Detail", expanded=False):
    st.write(taglist)
    st.write(query_str)
    st.write(start_ts)
    st.write(end_ts)

# Download in a CSV
with st.expander("📥 Download as CSV", expanded=False):
    # rows_choices = [1000, 10000, 100000, 1000000]
    # rows = st.selectbox('Select the number of rows to retrieve:', options=rows_choices)
    if st.button("Get download link"):
        st.write("Generating url...")
        
        temp_stage = "STAGE_TS_STREAMLIT"
        file_name = "bin_tsdata.csv"
        db = session.get_current_database()
        schema = session.get_current_schema()
        full_temp_stage = f"@{db}.{schema}.{temp_stage}"

        df_data.write.copy_into_location(
                # f"{temp_stage}/{filename}",
                f"{full_temp_stage}/{file_name}",
                header=True,
                overwrite=True,
                single=True,
                max_file_size=268435456,
                file_format_type="CSV",
                format_type_options={"COMPRESSION":"NONE"}
            )
        
        res = session.sql(
            f"select get_presigned_url({full_temp_stage}, '{file_name}', 600) as url"
        ).collect()

        url = res[0]["URL"]

        st.write(f"[📥 Download {file_name}]({url})")
        st.info("Right click and choose 'Open in new tab'")