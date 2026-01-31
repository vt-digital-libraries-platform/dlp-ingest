import logging, os, sys, urllib.parse
from ingest_classes.media_types_map import media_types_map

logger = logging.getLogger()

# Environment variables
env = {}
def set_environment(app_config=None):
    env["SCRIPT_ROOT"] = os.path.abspath(os.path.dirname(__file__))

    # try to load environment variables from app_config (passed when run via GUI, not CLI)
    if app_config is not None:
        for key, value in app_config.items():
            env[key] = value
            logger.info(f"{key}: {value}")
        return
    # otherwise, load from environment variables (when run via CLI or Lambda)
    else:
        env["AWS_SRC_BUCKET"] = os.getenv("AWS_SRC_BUCKET")
        env["AWS_DEST_BUCKET"] = os.getenv("AWS_DEST_BUCKET")
        env["COLLECTION_CATEGORY"] = os.getenv("COLLECTION_CATEGORY")
        env["COLLECTION_IDENTIFIER"] = os.getenv("COLLECTION_IDENTIFIER")
        env["COLLECTION_SUBDIRECTORY"] = os.getenv("COLLECTION_SUBDIRECTORY")
        env["ITEM_SUBDIRECTORY"] = os.getenv("ITEM_SUBDIRECTORY")
        env["REGION"] = os.getenv("REGION")
        env["DYNAMODB_TABLE_SUFFIX"] = os.getenv("DYNAMODB_TABLE_SUFFIX")
        env["DYNAMODB_NOID_TABLE"] = os.getenv("DYNAMODB_NOID_TABLE")
        env["DYNAMODB_FILE_CHAR_TABLE"] = os.getenv("DYNAMODB_FILE_CHAR_TABLE")
        env["APP_IMG_ROOT_PATH"] = os.getenv("APP_IMG_ROOT_PATH")
        env["NOID_SCHEME"] = os.getenv("NOID_SCHEME")
        env["NOID_NAA"] = os.getenv("NOID_NAA")
        env["LONG_URL_PATH"] = os.getenv("LONG_URL_PATH")
        env["SHORT_URL_PATH"] = os.getenv("SHORT_URL_PATH")
        env["MEDIA_TYPE"] = os.getenv("MEDIA_TYPE")

        # Booleans
        env["DRY_RUN"] = (
            os.getenv("DRY_RUN") is not None and os.getenv("DRY_RUN").lower() == "true"
        )
        if env["DRY_RUN"]:
            print("")
            print("DRY RUN ENABLED. NO RECORDS WILL BE WRITTEN TO DYNAMODB.")
            print("=========================================================")
        env["MEDIA_INGEST"] = (
            os.getenv("MEDIA_INGEST") is not None
            and os.getenv("MEDIA_INGEST").lower() == "true"
        )
        env["METADATA_INGEST"] = (
            os.getenv("METADATA_INGEST") is not None
            and os.getenv("METADATA_INGEST").lower() == "true"
        )
        env["GENERATE_THUMBNAILS"] = (
            os.getenv("GENERATE_THUMBNAILS") is not None
            and os.getenv("GENERATE_THUMBNAILS").lower() == "true"
        )
        env["IS_LAMBDA"] = (
            os.getenv("IS_LAMBDA") is not None and os.getenv("IS_LAMBDA").lower() == "true"
        )
        env["VERBOSE"] = (
            os.getenv("VERBOSE") is not None and os.getenv("VERBOSE").lower() == "true"
        )
        env["UPDATE_METADATA"] = (
            os.getenv("UPDATE_METADATA") is not None and os.getenv("UPDATE_METADATA").lower() == "true"
        )

        print(env)


def new_media_type_handler(env, filename, bucket):
    media_type = media_types_map[env["MEDIA_TYPE"]]
    return media_type["handler"](env, filename, bucket, media_type["assets"])


def main(event, context, csv_file=None, app_config=None):
    print("Starting ingest process at src.ingest.main...")
    set_environment(app_config)
    filename = None
    if event:
        bucket = event["Records"][0]["s3"]["bucket"]["name"]
        filename = urllib.parse.unquote_plus(
            event["Records"][0]["s3"]["object"]["key"], encoding="utf-8"
        )
    elif csv_file:
        bucket = None
        filename = csv_file
    media_type_handler = new_media_type_handler(env, filename, bucket)
    return media_type_handler.ingest()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 ingest.py <filename>")
        sys.exit(1)
    else:
        filename = "".join(sys.argv[1])
        main(None, None, filename)
