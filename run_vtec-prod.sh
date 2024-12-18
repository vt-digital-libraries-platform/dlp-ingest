#!/bin/bash

# uncomment this line to prevent MacOS from sleeping
caffeinate -imsu -w $$ &

pip3 install -r ./requirements.txt --quiet

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
# EXAMPLE METADATA FILES FROM OUR COLLECTIONS CAN BE FOUND IN THE './metadata_files' DIRECTORY
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!


##### prod #####
    ##### legacy #####
    # API_KEY="eSIaSK2L3y3hWpmSAzPpWaAgnghnRrEVabe5KbR4" \
    # API_ENDPOINT="https://2xmdyl893j.execute-api.us-east-1.amazonaws.com/Prod/" \
# VERBOSE="true" \
# AWS_SRC_BUCKET="vtlib-store" \
# AWS_DEST_BUCKET="img.cloud.lib.vt.edu" \
# COLLECTION_CATEGORY="federated" \
# COLLECTION_IDENTIFIER="LD5655.V8.T5" \
# COLLECTION_SUBDIRECTORY="" \
# ITEM_SUBDIRECTORY="" \
# REGION="us-east-1" \
# DYNAMODB_TABLE_SUFFIX="m6rxpkb73zehlhwrmyirtfbw3e-prod" \
# APP_IMG_ROOT_PATH="https://img.cloud.lib.vt.edu/" \
# NOID_SCHEME="ark:/" \
# NOID_NAA="53696" \
# LONG_URL_PATH="https://digital.lib.vt.edu/" \
# SHORT_URL_PATH="https://idn.lib.vt.edu/" \

# MEDIA_INGEST="false" \
# MEDIA_TYPE="pdf" \
# METADATA_INGEST="true" \
# GENERATE_THUMBNAILS="false" \
# DRY_RUN="false" \
# python3 lambda_function.py "/Users/whunter/dev/dlp/assets/LD5655.V8.T5/meta/20240920_LD5655.V8.T5_archive_metadata.csv"

##### dev/pprd #####
# AWS_SRC_BUCKET="ingest-dev-vtlib-store" \
# AWS_DEST_BUCKET="ingest-dev.img.cloud.lib.vt.edu" \
# APP_IMG_ROOT_PATH="https://d21nnzi4oh5qvs.cloudfront.net" \
# SHORT_URL_PATH=" https://ssxezg5su1.execute-api.us-east-1.amazonaws.com/" \
    ##### legacy #####
    # API_KEY="dfF3FKVu3C9MN24Kfz5Gm60admMuf6Yi41vRGv6X" \
    # API_ENDPOINT="https://ffzatqkiqe.execute-api.us-east-1.amazonaws.com/pprd/" \

#### dev ####
# LONG_URL_PATH="https://federated-dev.d2ysrrdhih4bgc.amplifyapp.com" \
# DYNAMODB_TABLE_SUFFIX="ws6c4ek7urgsflpjicndw27noa-dev" \
#### pprd ####
# LONG_URL_PATH="https://federated-pprd.dlp.cloud.lib.vt.edu" \
# DYNAMODB_TABLE_SUFFIX="63rcqhy47bdoxl5oszi623swoe-dlppprd" \

#comsub
#nonvtsrc
#univpub

# 3d_2diiif
VERBOSE="true" \
AWS_SRC_BUCKET="vtlib-store" \
AWS_DEST_BUCKET="img.cloud.lib.vt.edu" \
COLLECTION_CATEGORY="federated" \
COLLECTION_IDENTIFIER="vtec" \
COLLECTION_SUBDIRECTORY="" \
ITEM_SUBDIRECTORY="" \
REGION="us-east-1" \
DYNAMODB_TABLE_SUFFIX="m6rxpkb73zehlhwrmyirtfbw3e-prod" \
DYNAMODB_NOID_TABLE="mint" \
APP_IMG_ROOT_PATH="https://img.cloud.lib.vt.edu/" \
NOID_SCHEME="ark:/" \
NOID_NAA="53696" \
LONG_URL_PATH="https://digital.lib.vt.edu/" \
SHORT_URL_PATH="https://idn.lib.vt.edu/" \
MEDIA_INGEST="false" \
MEDIA_TYPE="3d_2diiif" \
METADATA_INGEST="true" \
GENERATE_THUMBNAILS="false" \
DRY_RUN="false" \
python3 lambda_function.py "/Users/whunter/dev/dlp/assets/vtec/meta/20241009_vtec_3d_archive_metadata.csv"

# "/Users/whunter/dev/dlp/assets/vtec/meta/20241009_vtec_3d_archive_metadata.csv"
# "/Users/whunter/dev/dlp/assets/vtec/meta/20241008_vtec_collection_metadata.csv"
# "/Users/whunter/dev/dlp/assets/vtec/meta/20241009_vtec_2d_archive_metadata.csv"