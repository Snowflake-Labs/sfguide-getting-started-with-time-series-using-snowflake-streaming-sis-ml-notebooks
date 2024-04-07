#!/bin/bash
conda env create -f ./.devcontainer/codespace_environment.yml
conda init
mkdir keys
rm keys/*
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out keys/rsa_key.p8 -nocrypt
openssl rsa -in keys/rsa_key.p8 -pubout -out keys/rsa_key.pub
chmod 0600 ".snowflake/config.toml"
chmod 0600 ".streamlit/config.toml"