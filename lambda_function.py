import json
import urllib.parse
import urllib.request
import requests
import boto3
import importlib
import io
import sys
import pandas as pd
from datetime import datetime, timezone
from dateutil.parser import parse
import uuid
import os
from boto3.dynamodb.conditions import Key, Attr
from botocore.response import StreamingBody
from lib_files.media_types.media_types_map import media_types_map

# Environment variables
env = {}
env["collection_category"] = os.getenv('Collection_Category')
env["region_name"] = os.getenv('REGION')
env["collection_table_name"] = os.getenv('DYNO_Collection_TABLE')
env["archive_table_name"] = os.getenv('DYNO_Archive_TABLE')
env["collectionmap_table_name"] = os.getenv('DYNO_Collectionmap_TABLE')
env["app_img_root_path"] = os.getenv('APP_IMG_ROOT_PATH')
env["noid_scheme"] = os.getenv('NOID_Scheme')
env["noid_naa"] = os.getenv('NOID_NAA')
env["long_url_path"] = os.getenv('LONG_URL_PATH')
env["short_url_path"] = os.getenv('SHORT_URL_PATH')
env["api_key"] = os.getenv('API_KEY')
env["api_endpoint"] = os.getenv('API_ENDPOINT')
env["media_ingest"] = os.getenv('MEDIA_INGEST')
env["media_type"] = os.getenv('MEDIA_TYPE')
env["metadata_ingest"] = os.getenv('METADATA_INGEST')


try:
    dyndb = boto3.resource('dynamodb', region_name=env["region_name"])
    env["archive_table"] = dyndb.Table(env["archive_table_name"])
    env["collection_table"] = dyndb.Table(env["collection_table_name"])
    env["collectionmap_table"] = dyndb.Table(env["collectionmap_table_name"])
except Exception as e:
    print(f"An error occurred: {str(e)}")
    raise e

headers_keys_file = 'lib_files/data/headers_keys.json'
try:
    with open(headers_keys_file, 'r') as f:
        headers_keys = json.load(f)
except Exception as e:
    print(f"An error occurred: {str(e)}")
    raise e


def import_metadata(filename, data, handler):
    print(filename)
    sys.exit(0)
    if 'index.csv' in filename:
        handler.batch_import_archives_with_path(data)
    elif 'collection_metadata.csv' in filename:
        handler.batch_import_collections(data)
    elif 'archive_metadata.csv' in filename:
        handler.batch_import_archives(data)


def get_media_type_handler(env, headers_keys):
    media_type = media_types_map[env['media_type']]
    return media_type["handler"](env, headers_keys)


def local_handler(filename):
    media_type_handler = get_media_type_handler(env, headers_keys)
    response = None
    try:
        body_encoded = open(filename).read().encode()
        stream = StreamingBody(io.BytesIO(body_encoded),len(body_encoded))
        response = {"Body": stream}
    except FileNotFoundError as e:
        print(f"An error occurred: {str(e)}")

    if env["metadata_ingest"] == "true" and bool(response):
        import_metadata(filename, response, media_type_handler)
    else:
        print("No metadata to import.")


def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(
        event['Records'][0]['s3']['object']['key'],
        encoding='utf-8')
    try:
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=bucket, Key=key)
        media_type_handler = get_media_type_handler(env, headers_keys)
        if env["metadata_ingest"] == "true":
            import_metadata(key, response, media_type_handler)
    except Exception as e:
        print(
            f"An error occurred importing {key} from bucket {bucket}: {str(e)}")
        raise e
    else:
        return {
            'statusCode': 200,
            'body': json.dumps('Finish metadata import.')
        }
    

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 lambda_function.py <filename>")
        sys.exit(1)
    else:
        filename = "".join(sys.argv[1])
        local_handler(filename)
        