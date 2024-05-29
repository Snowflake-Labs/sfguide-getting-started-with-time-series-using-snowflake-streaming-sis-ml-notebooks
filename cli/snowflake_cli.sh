#!/bin/bash

# Activate Environment
conda activate hol-timeseries

# Test Connections
snow --config-file=".snowflake/config.toml" connection test --connection="hol-timeseries"
snow --config-file=".snowflake/config.toml" connection test --connection="hol-timeseries-streamlit"

# Streamlit Deploy
snow --config-file=".snowflake/config.toml" streamlit deploy --replace --project "streamlit" --connection="hol-timeseries-streamlit"