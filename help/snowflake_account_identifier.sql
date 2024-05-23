-- Login as ACCOUNTADMIN
USE ROLE ACCOUNTADMIN;

-- Find your account details - <account_identifier>.snowflakecomputing.com
SELECT SYSTEM$ALLOWLIST();

/* EXTERNAL ACTIVITY

Use the SYSTEM$ALLOWLIST() command to find your Snowflake account "host" parameter where "type":"SNOWFLAKE_DEPLOYMENT_REGIONLESS": 
<ACCOUNT_IDENTIFIER>.snowflakecomputing.com

This host information will be used to update the deployment variable in the following files of your forked QuickStart repo:

.snowlfake/config.toml - update <ACCOUNT_IDENTIFIER> for both connections account parameter
iotstream/snowflake.properties - update <ACCOUNT_IDENTIFIER> for account and host parameter

*/