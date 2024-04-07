#!/bin/bash
# Test Connection
snow --config-file=".snowflake/config.toml" connection test --connection="hol-timeseries"
snow --config-file=".snowflake/config.toml" connection test --connection="hol-timeseries-streamlit"

# Ingestion
# Upload WITSML Data
snow --config-file=".snowflake/config.toml" object stage copy "./data/witsml/dataset" "@HOL_TIMESERIES.STAGING.STAGE_TS_WITSML/witsml" --overwrite --connection="hol-timeseries"

# Upload HDF5 Data
snow --config-file=".snowflake/config.toml" object stage copy "./data/open_DC_motor/dataset" "@HOL_TIMESERIES.STAGING.STAGE_TS_HDF5/open_DC_motor" --overwrite --connection="hol-timeseries"

# Streamlit
snow --config-file="../.snowflake/config.toml" streamlit deploy --replace --project "streamlit" --connection="hol-timeseries-streamlit"