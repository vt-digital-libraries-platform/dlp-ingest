#!/bin/bash
pip install -r requirements.txt

export INGEST_ENV_YAML="env_defaults.yml"
GUI=true python -m gunicorn --bind :8000 --workers=1 --threads=15 src.application