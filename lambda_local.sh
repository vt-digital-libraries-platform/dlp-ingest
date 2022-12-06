#!/bin/bash
pip install -r ./requirements.txt

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

Collection_Category=<Your Collection_Category> \
REGION=<Your AWS region> \
DYNO_Collection_TABLE=<Your Collection table name> \
DYNO_Archive_TABLE=<Your Archive table name> \
DYNO_Collectionmap_TABLE=<Your Collectionmap table name> \
APP_IMG_ROOT_PATH=<Your APP_IMG_ROOT_PATH> \
NOID_Scheme=<Your NOID_Scheme> \
NOID_NAA=<Your NOID_NAA> \
LONG_URL_PATH=<Your LONG_URL_PATH> \
SHORT_URL_PATH=<Your SHORT_URL_PATH> \
API_KEY=<Your API_KEY> \
API_ENDPOINT=<Your API_ENDPOINT> \
python3 lambda_function.py "/path/to/your/archive_metadata.csv"