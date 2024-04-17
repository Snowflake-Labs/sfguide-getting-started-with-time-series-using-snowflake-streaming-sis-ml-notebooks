#!/bin/bash
# Test Connection
snow --config-file=".snowflake/config.toml" connection test --connection="hol-timeseries"
snow --config-file=".snowflake/config.toml" connection test --connection="hol-timeseries-streamlit"

# Streamlit
snow --config-file=".snowflake/config.toml" streamlit deploy --replace --project "streamlit" --connection="hol-timeseries-streamlit"