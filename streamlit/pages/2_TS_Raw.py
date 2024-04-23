# Import libraries
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.types import *
from snowflake.snowpark import functions as F
from snowflake.snowpark import Window
from datetime import time, timedelta, datetime
import time
import altair as alt
import plotly.express as px
import streamlit as st
import pandas as pd
import datetime

# Get current session
session = get_active_session()

# Set page config
st.set_page_config(page_title="Time Series - Raw", layout="wide")

# Setup session state variables
if "times_refreshed" not in st.session_state:
    st.session_state["times_refreshed"] = 0
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
    st.session_state["sample"] = 500

# Page title
st.title('Time Series - Raw')

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
    SELECT TAGNAME, TAGUNITS, TAGDATATYPE, TAGDESCRIPTION FROM TS_TAG_REFERENCE
    WHERE TAGNAME IN ({})
    """.format(", ".join(f"'{tag}'" for tag in taglist))
    df_tag_metadata = session.sql(query).toPandas()
else:
    df_tag_metadata = pd.DataFrame(columns=['TAGNAME', 'TAGUNITS', 'TAGDATATYPE', 'TAGDESCRIPTION'])

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

# TS Table Query Builder
table_query_str = '''
SELECT TAGNAME, TIMESTAMP, VALUE_NUMERIC AS VALUE
FROM TS_TAG_READINGS
WHERE TIMESTAMP >= TIMESTAMP '{start_ts}'
AND TIMESTAMP < TIMESTAMP '{end_ts}'
AND TAGNAME IN {taglist}
ORDER BY TAGNAME, TIMESTAMP
'''

chart_query_str = '''
SELECT DATA.TAGNAME, LTTB.TIMESTAMP::VARCHAR::TIMESTAMP_NTZ AS TIMESTAMP, LTTB.VALUE::FLOAT AS VALUE
FROM (
{table_query}
) AS DATA
CROSS JOIN TABLE(FUNCTION_TS_LTTB(DATE_PART(EPOCH_NANOSECOND, DATA.TIMESTAMP), DATA.VALUE, {sample}) OVER (PARTITION BY DATA.TAGNAME ORDER BY DATA.TIMESTAMP)) AS LTTB
ORDER BY TAGNAME, TIMESTAMP'''

# TS dataframe with query variables
df_table_data = session.sql(
    table_query_str \
        .replace("{start_ts}", str(start_ts)) \
        .replace("{end_ts}", str(end_ts)) \
        .replace("{taglist}", str(tuple(filter)))
        )

df_chart_data = session.sql(
    chart_query_str \
        .replace("{table_query}", str( \
            table_query_str \
                .replace("{start_ts}", str(start_ts)) \
                .replace("{end_ts}", str(end_ts)) \
                .replace("{taglist}", str(tuple(filter))) \
        ))
        .replace("{sample}", str(sample)) \
        )

# Create chart plot
if taglist:
    with st.container():
        st.subheader('Tag Metadata')
        st.dataframe(df_tag_metadata, hide_index=True, use_container_width=True)

        alt_chart_1 = (
            alt.Chart(df_chart_data.to_pandas())
                .mark_line(point=False, interpolate="linear")
                .encode(
                    x=alt.X("utcyearmonthdatehoursminutesseconds(TIMESTAMP):T", title="TIMESTAMP", axis=alt.Axis(format='%Y-%m-%d %H:%M')),
                    y=alt.Y("VALUE"),
                    color=alt.Color("TAGNAME"),
                    order="TIMESTAMP",
                    #text=alt.Text("TOTAL_COST:Q", format=",.0f"),
                )
                .interactive()
        )
        st.altair_chart(alt_chart_1, use_container_width=True)

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

if taglist:
    with st.expander("📥 Download as CSV - " + str(df_table_data.count()) + " rows", expanded=False):
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
                f"select get_presigned_url({full_temp_stage}, '{file_name}', 3600) as url"
            ).collect()

            url = res[0]["URL"]

            st.write(f"[📥 Download {file_name}]({url})")
            st.info("Right click and choose 'Open in new tab'")

if taglist:
    with st.expander("🔍 Supporting Detail", expanded=False):
        st.subheader('Query:')
        st.code(table_query_str \
            .replace("{start_ts}", str(start_ts)) \
            .replace("{end_ts}", str(end_ts)) \
            .replace("{taglist}", str(tuple(filter))),
            language='sql'
            )

if taglist:
    with st.expander("♻️ Refresh Mode", expanded=False):
        refresh_section = st.columns((1, 1, 6))
        st.session_state["times_refreshed"] += 1
        with refresh_section[1]:
            st.info(f"**Times Refreshed** : {st.session_state['times_refreshed']} ")
        with refresh_section[0]:
            refresh_mode = st.radio(
                "Refresh Automatically?", options=["Yes", "No"], index=1, horizontal=True
            )

        if refresh_mode == "Yes":
            refresh_section[2].success("Data will refresh every minute")
            progress_b = refresh_section[2].progress(0)
            for percent_complete in range(100):
                time.sleep(0.6)
                progress_b.progress(percent_complete + 1)

            st.experimental_rerun()

        if refresh_mode == "No":
            refresh = st.button("Refresh")
            if refresh:
                st.experimental_rerun()