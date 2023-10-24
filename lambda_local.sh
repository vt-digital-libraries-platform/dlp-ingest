#!/bin/bash
pip install -q -r ./requirements.txt

# In order to run you must set the following Environment variables below.
# Environment variable details can be found here: 
# https://github.com/vt-digital-libraries-platform/S3toDDB


# You must also customize the argument to the python call in the last line
# to point to your metadata file (csv format)


# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
#
# NOTE: RUNNING THIS SCRIPT LOCALLY WILL ACTUALLY MODIFY THE DYNAMODB TABLES THAT
# YOU SPECIFY IN THE ENVIRONMENT VARIABLES BELOW. PLEASE MAKE SURE YOU ARE USING
# TABLES ATTACHED TO A DEVELOPMENT/TESTING AMPLIFY BACKEND
#
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

Collection_Category="default" \
REGION="us-east-1" \
DYNO_Archive_TABLE="Archive-w2ps5pqmqja4rbgtw46jp4nsvy-ingest" \
DYNO_Collection_TABLE="Collection-w2ps5pqmqja4rbgtw46jp4nsvy-ingest" \
DYNO_Collectionmap_TABLE="Collectionmap-w2ps5pqmqja4rbgtw46jp4nsvy-ingest" \
APP_IMG_ROOT_PATH="https://img.cloud.lib.vt.edu/demo/" \
NOID_Scheme="ark:/" \
NOID_NAA="53696" \
LONG_URL_PATH="https://demo-dev.dlp.cloud.lib.vt.edu/" \
SHORT_URL_PATH="https://ssxezg5su1.execute-api.us-east-1.amazonaws.com/Prod/" \
API_KEY="2PtUFKWiwU5qAR7w4cOhb3vTRZthgkeJ6JNmTXyf" \
API_ENDPOINT="https://ffzatqkiqe.execute-api.us-east-1.amazonaws.com/Prod/" \
MEDIA_INGEST="true" \
MEDIA_TYPE="3d" \
METADATA_INGEST="true" \
python3 lambda_function.py "/Users/whunter/dev/dlp/dlp-ingest/examples/metadata/2023-09-27_example_archive_metadata.csv"