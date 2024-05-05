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
st.set_page_config(page_title="Time Series - Binning", layout="wide")

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
st.title('Time Series - Binning')

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

# Agg name and column query
agg_options = {
    'AVG': "AVG(VALUE_NUMERIC)",
    'MIN': "MIN(VALUE_NUMERIC)",
    'MAX': "MAX(VALUE_NUMERIC)",
    'SUM': "SUM(VALUE_NUMERIC)",
    'COUNT': "COUNT(VALUE)::FLOAT",
    'COUNT_DISTINCT': "COUNT(DISTINCT VALUE)::FLOAT",
    'STDDEV': "STDDEV(VALUE_NUMERIC)",
    'VARIANCE': "VARIANCE(VALUE_NUMERIC)",
    'PERCENTILE_50': "APPROX_PERCENTILE(VALUE_NUMERIC, 0.5)",
    'PERCENTILE_95': "APPROX_PERCENTILE(VALUE_NUMERIC, 0.95)"
}

# BINNING - Configuration
st.sidebar.markdown('## Binning Configuration')
selected_agg = st.sidebar.selectbox('Select Aggregation Method', list(agg_options.keys()))
bin_range = st.sidebar.number_input('Enter Bin Range', min_value=1, value=1)
interval_unit = st.sidebar.selectbox('Select Interval Unit', ['MINUTE', 'SECOND', 'HOUR', 'DAY', 'WEEK', 'MONTH'])
label_position = st.sidebar.radio('Label Position', ['START', 'END'], index=1, horizontal=True)

# Table and chart query definitions
table_query_str = '''
SELECT TAGNAME||'~'||'{selected_agg}'||'_'||'{bin_range}'||'{interval_unit}' AS TAGNAME, TIME_SLICE(TIMESTAMP, {bin_range}, '{interval_unit}', '{label_position}') AS TIMESTAMP, {agg_options[selected_agg]} AS VALUE
FROM TS_TAG_READINGS
WHERE TIMESTAMP >=  '{start_ts}' AND TIMESTAMP < '{end_ts}' AND TAGNAME IN {tag_tuple}
GROUP BY TAGNAME, TIME_SLICE(TIMESTAMP, {bin_range}, '{interval_unit}', '{label_position}')
ORDER BY TAGNAME, TIMESTAMP
'''

chart_query_str = f'''
SELECT DATA.TAGNAME, LTTB.TIMESTAMP::VARCHAR::TIMESTAMP_NTZ AS TIMESTAMP, LTTB.VALUE::FLOAT AS VALUE
FROM (
    SELECT TAGNAME||'~'||'{selected_agg}'||'~'||'{bin_range}'||'{interval_unit}' AS TAGNAME, TIME_SLICE(TIMESTAMP, {bin_range}, '{interval_unit}', '{label_position}') AS TIMESTAMP, {agg_options[selected_agg]} AS VALUE
    FROM TS_TAG_READINGS
    WHERE TIMESTAMP >=  '{start_ts}' AND TIMESTAMP < '{end_ts}' AND TAGNAME IN {tag_tuple}
    GROUP BY TAGNAME, TIME_SLICE(TIMESTAMP, {bin_range}, '{interval_unit}', '{label_position}')
    ORDER BY TAGNAME, TIMESTAMP
) AS DATA
CROSS JOIN TABLE(FUNCTION_TS_LTTB(DATE_PART(EPOCH_NANOSECOND, DATA.TIMESTAMP), DATA.VALUE, {sample}) OVER (PARTITION BY DATA.TAGNAME ORDER BY DATA.TIMESTAMP)) AS LTTB
ORDER BY TAGNAME, TIMESTAMP
'''

# Generate a SQL-compatible tuple from the tag list
tag_tuple = str(tuple(taglist)) if len(taglist) > 1 else f"('{taglist[0]}')" if taglist else "()"

# Dataframe definitions for table and chart
df_table_data = session.sql(
    table_query_str \
        .replace("{selected_agg}", str(selected_agg)) \
        .replace("{start_ts}", str(start_ts)) \
        .replace("{end_ts}", str(end_ts)) \
        .replace("{taglist}", str(tuple(filter))) \
        .replace("{bin_range}", str(bin_range)) \
        .replace("{interval_unit}", str(interval_unit)) \
        .replace("{label_position}", str(label_position)) \
        .replace("{tag_tuple}", str(tag_tuple)) \
        .replace("{agg_options[selected_agg]}", str(agg_options[selected_agg]))
        )

df_chart_data = session.sql(
    chart_query_str \
        .replace("{start_ts}", str(start_ts)) \
        .replace("{end_ts}", str(end_ts)) \
        .replace("{taglist}", str(tuple(filter))) \
        .replace("{sample}", str(sample))
        .replace("{bin_range}", str(bin_range)) \
        .replace("{interval_unit}", str(interval_unit)) \
        .replace("{label_position}", str(label_position)) \
        .replace("{tag_tuple}", str(tag_tuple)) \
        .replace("{agg_options[selected_agg]}", str(agg_options[selected_agg]))
        )

# Analytic outputs in a container
if taglist:
    with st.container():
        # Metadata Table
        st.subheader('Tag Metadata')
        st.dataframe(df_tag_metadata, hide_index=True, use_container_width=True)

        # Downsampled Chart
        st.write(f"##### AGGREGATION: {selected_agg}")
        st.line_chart(df_chart_data.to_pandas(), x="TIMESTAMP", y="VALUE", color="TAGNAME", use_container_width=True)

        # Readings Table
        st.subheader('Tag Data')
        rows_choices = [100, 1000, 10000, 100000]
        rows = st.selectbox('Select the number of rows to retrieve:', options=rows_choices)
        sorter = st.toggle('Data Order', value=False)
        if sorter:
            st.write("TIMESTAMP: Ascending")
        else:
            st.write("TIMESTAMP: Descending")
        st.dataframe(df_table_data.sort(F.col("TIMESTAMP"), ascending=sorter).limit(rows).collect(), hide_index=True, use_container_width=True)

else:
    st.write("❄️ Select one or more tags to view data.")

# Download data
if taglist:
    with st.expander("📥 Download as CSV - " + str(df_table_data.count()) + " rows", expanded=False):
        if st.button("Get download link"):
            st.write("Generating url...")
            
            temp_stage = "STAGE_TS_STREAMLIT"
            file_name = "bin_tsdata.csv"
            db = session.get_current_database()
            schema = session.get_current_schema()
            full_temp_stage = f"@{db}.{schema}.{temp_stage}"

            df_table_data.write.copy_into_location(
                    f"{full_temp_stage}/{file_name}",
                    header=True,
                    overwrite=True,
                    single=True,
                    max_file_size=268435456,
                    file_format_type="CSV",
                    format_type_options={"COMPRESSION":"NONE"}
                )
            
            res = session.sql(
                f"select get_presigned_url({full_temp_stage}, '{file_name}', 3600) as url"
            ).collect()

            url = res[0]["URL"]

            st.write(f"[📥 Download {file_name}]({url})")
            st.info("Right click and choose 'Open in new tab'")

# Query detail
if taglist:
    with st.expander("🔍 Supporting Detail", expanded=False):
        st.subheader('Query:')
        st.code(table_query_str \
            .replace("{selected_agg}", str(selected_agg)) \
            .replace("{start_ts}", str(start_ts)) \
            .replace("{end_ts}", str(end_ts)) \
            .replace("{taglist}", str(tuple(filter))) \
            .replace("{bin_range}", str(bin_range)) \
            .replace("{interval_unit}", str(interval_unit)) \
            .replace("{label_position}", str(label_position)) \
            .replace("{tag_tuple}", str(tag_tuple)) \
            .replace("{agg_options[selected_agg]}", str(agg_options[selected_agg]))
            , language="sql")

# Refresh toggle
if taglist:
    with st.expander("♻️ Refresh Mode", expanded=False):
        refresh_section = st.columns((1, 6))
        st.session_state["times_refreshed"] += 1
        with refresh_section[0]:
            refresh_mode = st.toggle(
                "Auto Refresh", value=st.session_state["refresh_mode"]
            )
            st.session_state["refresh_mode"] = refresh_mode

        if refresh_mode == True:
            refresh_section[1].success("Data will refresh every minute")
            progress_b = refresh_section[1].progress(0)
            for percent_complete in range(100):
                time.sleep(0.6)
                progress_b.progress(percent_complete + 1)

            st.session_state["start_date"] = datetime.datetime.now(datetime.timezone.utc) - timedelta(hours=1)
            st.session_state["start_time"] = datetime.datetime.now(datetime.timezone.utc) - timedelta(hours=1)
            st.session_state["end_date"] = datetime.datetime.now(datetime.timezone.utc) + timedelta(hours=1)
            st.session_state["end_time"] = datetime.datetime.now(datetime.timezone.utc) + timedelta(hours=1)
            st.experimental_rerun()

        if refresh_mode == False:
            refresh = st.button("Refresh")
            if refresh:
                st.session_state["times_refreshed"] == 0
                st.experimental_rerun()