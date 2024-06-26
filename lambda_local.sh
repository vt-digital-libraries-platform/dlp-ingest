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

AWS_DEST_BUCKET="<AWS bucket where results will be written>" \
COLLECTION_CATEGORY="<Your Collection_Category>" \
COLLECTION_IDENTIFIER="fchs" \
REGION="<Your AWS region>" \
DYNAMODB_TABLE_SUFFIX="<Dynamo tables env id>" \
APP_IMG_ROOT_PATH="<Base path for site assets>" \
NOID_SCHEME="ark:/" \
NOID_NAA="53696" \
LONG_URL_PATH="<Base url for your site>" \
SHORT_URL_PATH="<NOID resolver url>" \
API_KEY="<API key for your noid minter>" \
API_ENDPOINT="<NOID minter>" \
MEDIA_INGEST="<Boolean>" \
MEDIA_TYPE="<Your Media Type>" \
METADATA_INGEST="<Boolean>" \
python3 lambda_function.py "./examples/20240211_fchs_archive_metadata.csv"
