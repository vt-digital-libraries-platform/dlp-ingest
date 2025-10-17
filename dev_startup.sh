#!/bin/bash
pip install -r requirements.txt

cd ./src
rm -rf dlp_ingest
git clone --branch ui https://github.com/vt-digital-libraries-platform/dlp-ingest dlp_ingest
cd ..
export INGEST_ENV_YAML="env_defaults.yml"
GUI=true python -m gunicorn --bind :8000 --workers=1 --threads=15 src.application