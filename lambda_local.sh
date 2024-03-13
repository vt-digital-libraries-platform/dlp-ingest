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
#
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

# COLLLECTION_CATEGORY="<Your Collection_Category>" \
# REGION="<Your AWS region>" \
# DYNO_Collection_TABLE="<Your Collection table name>" \
# DYNO_Archive_TABLE="<Your Archive table name>" \
# DYNO_Collectionmap_TABLE="<Your Collectionmap table name>" \
# APP_IMG_ROOT_PATH="<Your APP_IMG_ROOT_PATH>" \
# NOID_Scheme="<Your NOID_Scheme>" \
# NOID_NAA="<Your NOID_NAA>" \
# LONG_URL_PATH="<Your LONG_URL_PATH>" \
# SHORT_URL_PATH="<Your SHORT_URL_PATH>" \
# API_KEY="<Your API_KEY>" \
# API_ENDPOINT="<Your API_ENDPOINT>" \
# MEDIA_INGEST="<Boolean>" \
# MEDIA_TYPE="<Your Media Type>" \
# METADATA_INGEST="<Boolean>" \
# python3 lambda_function.py "/path/to/your/archive_metadata.csv"

AWS_DEST_BUCKET="ingest-dev.img.cloud.lib.vt.edu" \
COLLECTION_CATEGORY="swva" \
COLLECTION_IDENTIFIER="fchs" \
REGION="us-east-1" \
DYNAMODB_TABLE_SUFFIX="7haaux5rgzdctf4oru33wi3hfa-ingsttabls" \
APP_IMG_ROOT_PATH="https://d21nnzi4oh5qvs.cloudfront.net/" \
NOID_SCHEME="ark:/" \
NOID_NAA="53696" \
LONG_URL_PATH="https://swva-dev.dlp.cloud.lib.vt.edu" \
SHORT_URL_PATH="https://ssxezg5su1.execute-api.us-east-1.amazonaws.com/Prod/" \
API_KEY="2PtUFKWiwU5qAR7w4cOhb3vTRZthgkeJ6JNmTXyf" \
API_ENDPOINT="https://ffzatqkiqe.execute-api.us-east-1.amazonaws.com/Prod/" \
MEDIA_INGEST="<Boolean>" \
MEDIA_TYPE="<Your Media Type>" \
METADATA_INGEST="<Boolean>" \
python3 lambda_function.py "./examples/20240211_fchs_archive_metadata.csv"