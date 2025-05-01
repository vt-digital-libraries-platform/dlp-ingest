import urllib.parse
import sys
import os
from src.media_types.media_types_map import media_types_map

# Environment variables
env = {}
env["script_root"] = os.path.abspath(os.path.dirname(__file__))
env["aws_src_bucket"] = os.getenv("AWS_SRC_BUCKET")
env["aws_dest_bucket"] = os.getenv("AWS_DEST_BUCKET")
env["collection_category"] = os.getenv("COLLECTION_CATEGORY")
env["collection_identifier"] = os.getenv("COLLECTION_IDENTIFIER")
env["collection_subdirectory"] = os.getenv("COLLECTION_SUBDIRECTORY")
env["item_subdirectory"] = os.getenv("ITEM_SUBDIRECTORY")
env["region_name"] = os.getenv("REGION")
env["dynamodb_table_suffix"] = os.getenv("DYNAMODB_TABLE_SUFFIX")
env['dynamodb_noid_table'] = os.getenv("DYNAMODB_NOID_TABLE")
env["app_img_root_path"] = os.getenv("APP_IMG_ROOT_PATH")
env["noid_scheme"] = os.getenv("NOID_SCHEME")
env["noid_naa"] = os.getenv("NOID_NAA")
env["long_url_path"] = os.getenv("LONG_URL_PATH")
env["short_url_path"] = os.getenv("SHORT_URL_PATH")
env["media_type"] = os.getenv("MEDIA_TYPE")

# Booleans
env["dry_run"] = (
    os.getenv("DRY_RUN") is not None and os.getenv("DRY_RUN").lower() == "true"
)
if env["dry_run"]:
    print("")
    print("DRY RUN ENABLED. NO RECORDS WILL BE WRITTEN TO DYNAMODB.")
    print("=========================================================")
env["media_ingest"] = (
    os.getenv("MEDIA_INGEST") is not None
    and os.getenv("MEDIA_INGEST").lower() == "true"
)
env["metadata_ingest"] = (
    os.getenv("METADATA_INGEST") is not None
    and os.getenv("METADATA_INGEST").lower() == "true"
)
env["generate_thumbnails"] = (
    os.getenv("GENERATE_THUMBNAILS") is not None
    and os.getenv("GENERATE_THUMBNAILS").lower() == "true"
)
env["is_lambda"] = (
    os.getenv("IS_LAMBDA") is not None and os.getenv("IS_LAMBDA").lower() == "true"
)
env["verbose"] = (
    os.getenv("VERBOSE") is not None and os.getenv("VERBOSE").lower() == "true"
)

# The environment variable should probably have "update" in it, to differentiate the process from create
env["update_metadata"] = (
    os.getenv("UPDATE_METADATA") is not None and os.getenv("UPDATE_METADATA").lower() == "true"
)


def new_media_type_handler(env, filename, bucket):
    media_type = media_types_map[env["media_type"]]
    return media_type["handler"](env, filename, bucket, media_type["assets"])


def main(event, context, csv_file=None):
    filename = None
    if event and env["is_lambda"]:
        bucket = event["Records"][0]["s3"]["bucket"]["name"]
        filename = urllib.parse.unquote_plus(
            event["Records"][0]["s3"]["object"]["key"], encoding="utf-8"
        )
    elif csv_file and not env["is_lambda"]:
        bucket = None
        filename = csv_file
    media_type_handler = new_media_type_handler(env, filename, bucket)
    return media_type_handler.ingest()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 lambda_function.py <filename>")
        sys.exit(1)
    else:
        filename = "".join(sys.argv[1])
        main(None, None, filename)
