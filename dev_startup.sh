#!/bin/bash
pip install -r requirements.txt

INGEST_ENV_YAML="env_defaults.yml" \
FLASK_SECRET=`python -c 'import os; print(os.urandom(24))'` \
COGNITO_APP_CLIENT_SECRET="163q19s9uqsqdus627mbhg4m6ajnpeepheonacflg17q180volbn" \
python -m gunicorn --bind=0.0.0.0:8000 --workers 1 application