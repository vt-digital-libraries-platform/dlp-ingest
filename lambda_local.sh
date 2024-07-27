#!/bin/bash
pip install -r ./requirements.txt --quiet

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
# ...OR YOU REALLY KNOW WHAT YOU'RE DOING
#
# EXAMPLE METADATA FILES FROM OUR COLLECTIONS CAN BE FOUND IN THE './examples' DIRECTORY
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!


##### prod #####
AWS_SRC_BUCKET="vtlib-store" \
AWS_DEST_BUCKET="img.cloud.lib.vt.edu" \
COLLECTION_CATEGORY="federated" \
COLLECTION_IDENTIFIER="sfd" \
COLLECTION_SUBDIRECTORY="" \
ITEM_SUBDIRECTORY="" \
REGION="us-east-1" \
DYNAMODB_TABLE_SUFFIX="m6rxpkb73zehlhwrmyirtfbw3e-prod" \
APP_IMG_ROOT_PATH="https://img.cloud.lib.vt.edu/" \
NOID_SCHEME="ark:/" \
NOID_NAA="53696" \
LONG_URL_PATH="https://digital.lib.vt.edu/" \
SHORT_URL_PATH="https://idn.lib.vt.edu/" \
API_KEY="eSIaSK2L3y3hWpmSAzPpWaAgnghnRrEVabe5KbR4" \
API_ENDPOINT="https://2xmdyl893j.execute-api.us-east-1.amazonaws.com/Prod/" \
MEDIA_INGEST="false" \
MEDIA_TYPE="iiif" \
METADATA_INGEST="true" \
DRY_RUN="false" \
python3 lambda_function.py "./examples/20240705_sfd_archive_metadata.csv"

##### pprd #####
# AWS_SRC_BUCKET="ingest-dev-vtlib-store" \
# AWS_DEST_BUCKET="ingest-dev.img.cloud.lib.vt.edu" \
# COLLECTION_CATEGORY="federated" \
# COLLECTION_IDENTIFIER="sfd" \
# COLLECTION_SUBDIRECTORY="" \
# ITEM_SUBDIRECTORY="" \
# REGION="us-east-1" \
# DYNAMODB_TABLE_SUFFIX="ws6c4ek7urgsflpjicndw27noa-dev" \
# APP_IMG_ROOT_PATH="https://d21nnzi4oh5qvs.cloudfront.net/" \
# NOID_SCHEME="ark:/" \
# NOID_NAA="53696" \
# LONG_URL_PATH="https://federated-dev.dlp.cloud.lib.vt.edu/" \
# SHORT_URL_PATH="https://ssxezg5su1.execute-api.us-east-1.amazonaws.com/Prod" \
# API_KEY="2PtUFKWiwU5qAR7w4cOhb3vTRZthgkeJ6JNmTXyf" \
# API_ENDPOINT="https://ffzatqkiqe.execute-api.us-east-1.amazonaws.com/Prod/" \
# MEDIA_INGEST="false" \
# MEDIA_TYPE="iiif" \
# METADATA_INGEST="true" \
# DRY_RUN="false" \
# python3 lambda_function.py "./examples/sfd_apr5_archive_metadata.csv"