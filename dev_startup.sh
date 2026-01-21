#!/bin/bash
pip install -r requirements.txt

LOG_FILE="log/application.log" \
INGEST_ENV_YAML="env_defaults.yml" \
APP_SECRET="163q19s9uqsqdus627mbhg4m6ajnpeepheonacflg17q180volbn" \
python -m gunicorn --bind=0.0.0.0:8000 --workers 1 application