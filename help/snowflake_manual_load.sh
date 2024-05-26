#!/bin/bash

# Activate Environment
conda activate hol-timeseries

# Manual Data Load
snow --config-file=".snowflake/config.toml" stage create "STAGE_TS_DATA" --connection="hol-timeseries"
snow --config-file=".snowflake/config.toml" stage copy ./iotstream/STREAM_DATA.CSV @STAGE_TS_DATA --overwrite --connection="hol-timeseries"
snow --config-file=".snowflake/config.toml" stage copy ./help/snowflake_manual_load.sql @STAGE_TS_DATA --overwrite --connection="hol-timeseries"
snow --config-file=".snowflake/config.toml" stage execute "@STAGE_TS_DATA" --connection="hol-timeseries"

# List Staged Files
snow --config-file=".snowflake/config.toml" stage list-files @STAGE_TS_DATA --connection="hol-timeseries"