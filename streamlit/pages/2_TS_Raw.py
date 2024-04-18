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
import contextlib
import textwrap
import traceback

# Set page config
st.set_page_config(layout="wide")
st.title('Time Series - Raw')

# Get current session
session = get_active_session()

# Get a list of all tags
df_tags = session.table('TS_TAG_REFERENCE').select(F.col('TAGNAME')).toPandas()

st.sidebar.markdown('## Tag Selection')
taglist = st.sidebar.multiselect('Select Tag Names', df_tags)
filter = (*taglist, "", "")

# Correct way to handle the SQL tuple for taglist
if len(taglist) == 1:
    tag_tuple = f"('{taglist[0]}')"
elif taglist:
    tag_tuple = str(tuple(taglist))
else:
    tag_tuple = "('')"  # Default or error case handling: no tags selected

# Fetch tag metadata for selected tags 
if taglist:
    query = """
    SELECT TAGNAME, TAGUNITS, TAGDATATYPE FROM TS_TAG_REFERENCE
    WHERE TAGNAME IN ({})
    """.format(", ".join(f"'{tag}'" for tag in taglist))
    df_tag_metadata = session.sql(query).toPandas()
else:
    df_tag_metadata = pd.DataFrame(columns=['TAGNAME', 'TAGUNITS', 'TAGDATATYPE'])

st.subheader('Tag Metadata')
st.dataframe(df_tag_metadata, hide_index=True, use_container_width=True)

# Set time range
st.sidebar.markdown('## Time Selection (UTC)')
start_date = st.sidebar.date_input('Start Date', datetime.datetime.now(datetime.timezone.utc) - timedelta(hours=8), datetime.date(2024, 1, 1), datetime.date(2030, 12, 31))
end_date = st.sidebar.date_input('End Date', datetime.datetime.now(datetime.timezone.utc), datetime.date(2024, 1, 1), datetime.date(2030, 12, 31))
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

# TS Query Builder
chart_query_str = '''
SELECT DATA.TAGNAME, LTTB.TIMESTAMP::VARCHAR::TIMESTAMP_NTZ AS TIMESTAMP, LTTB.VALUE::FLOAT AS VALUE
FROM (
SELECT TAGNAME, TIMESTAMP, VALUE_NUMERIC AS VALUE
FROM TS_TAG_READINGS
WHERE TIMESTAMP >= TIMESTAMP '{start_ts}'
AND TIMESTAMP < TIMESTAMP '{end_ts}'
AND TAGNAME IN {taglist}
) AS DATA
CROSS JOIN TABLE(FUNCTION_TS_LTTB(DATE_PART(EPOCH_NANOSECOND, DATA.TIMESTAMP), DATA.VALUE, {sample}) OVER (PARTITION BY DATA.TAGNAME ORDER BY DATA.TIMESTAMP)) AS LTTB
ORDER BY TAGNAME, TIMESTAMP'''

table_query_str = '''
SELECT TAGNAME, TIMESTAMP, VALUE_NUMERIC AS VALUE
FROM TS_TAG_READINGS
WHERE TIMESTAMP >= TIMESTAMP '{start_ts}'
AND TIMESTAMP < TIMESTAMP '{end_ts}'
AND TAGNAME IN {taglist}
ORDER BY TAGNAME, TIMESTAMP
'''

# TS dataframe with query variables
df_chart_data = session.sql(
    chart_query_str \
        .replace("{start_ts}", str(start_ts)) \
        .replace("{end_ts}", str(end_ts)) \
        .replace("{taglist}", str(tuple(filter))) \
        .replace("{sample}", str(sample)) \
        )

df_table_data = session.sql(
    table_query_str \
        .replace("{start_ts}", str(start_ts)) \
        .replace("{end_ts}", str(end_ts)) \
        .replace("{taglist}", str(tuple(filter)))
        )

# Create chart plot
with st.container():
    alt_chart_1 = alt.Chart(df_chart_data.to_pandas()).mark_line().encode(x="TIMESTAMP", y="VALUE", color="TAGNAME").interactive()
    st.altair_chart(alt_chart_1, use_container_width=True)
    # fig = px.line(df_data, x='TIMESTAMP', y='VALUE_NUMERIC', color='TAGNAME')
    # st.plotly_chart(fig, use_container_width=True, render='svg')

    st.subheader('Tag Data')

    rows_choices = [100, 1000, 10000, 100000]
    rows = st.selectbox('Select the number of rows to retrieve:', options=rows_choices)
    
    st.dataframe(df_table_data.limit(rows).collect(), hide_index=True, use_container_width=True)

with st.expander("📥 Download as CSV", expanded=False):
    if st.button("Get download link"):
        st.write("Generating url...")
        
        temp_stage = "STAGE_TS_STREAMLIT"
        file_name = "tsdata_raw.csv"
        db = session.get_current_database()
        schema = session.get_current_schema()
        full_temp_stage = f"@{db}.{schema}.{temp_stage}"

        df_table_data.write.copy_into_location(
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

with st.expander("Supporting Detail", expanded=False):
    st.subheader('Query:')
    st.write(table_query_str \
        .replace("{start_ts}", str(start_ts)) \
        .replace("{end_ts}", str(end_ts)) \
        .replace("{taglist}", str(tuple(filter)))
        )