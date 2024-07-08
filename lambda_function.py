import os
import sys
import urllib.parse
from lib_files.media_types.media_types_map import media_types_map

class EnvConfig:
    def __init__(self):
        self.script_root = os.path.abspath(os.path.dirname(__file__))
        self.aws_src_bucket = os.getenv("AWS_SRC_BUCKET")
        self.aws_dest_bucket = os.getenv("AWS_DEST_BUCKET")
        self.collection_category = os.getenv("COLLECTION_CATEGORY")
        self.collection_identifier = os.getenv("COLLECTION_IDENTIFIER")
        self.collection_subdirectory = os.getenv("COLLECTION_SUBDIRECTORY")
        self.item_subdirectory = os.getenv("ITEM_SUBDIRECTORY")
        self.region_name = os.getenv("REGION")
        self.dynamodb_table_suffix = os.getenv("DYNAMODB_TABLE_SUFFIX")
        self.app_img_root_path = os.getenv("APP_IMG_ROOT_PATH")
        self.noid_scheme = os.getenv("NOID_SCHEME")
        self.noid_naa = os.getenv("NOID_NAA")
        self.long_url_path = os.getenv("LONG_URL_PATH")
        self.short_url_path = os.getenv("SHORT_URL_PATH")
        self.api_key = os.getenv("API_KEY")
        self.api_endpoint = os.getenv("API_ENDPOINT")
        self.media_type = os.getenv("MEDIA_TYPE")
        self.dry_run = os.getenv("DRY_RUN") is not None and os.getenv("DRY_RUN").lower() == "true"
        self.media_ingest = os.getenv("MEDIA_INGEST") is not None and os.getenv("MEDIA_INGEST").lower() == "true"
        self.metadata_ingest = os.getenv("METADATA_INGEST") is not None and os.getenv("METADATA_INGEST").lower() == "true"
        self.is_lambda = os.getenv("IS_LAMBDA") is not None and os.getenv("IS_LAMBDA").lower() == "true"
        self.verbose = os.getenv("VERBOSE") is not None and os.getenv("VERBOSE").lower() == "true"
        
        if self.dry_run:
            print("\nDRY RUN ENABLED. NO RECORDS WILL BE WRITTEN TO DYNAMODB.\n" + "=" * 57)


class MediaTypeHandler:
    def __init__(self, env, filename, bucket):
        self.env = env
        self.filename = filename
        self.bucket = bucket
        self.handler = self._get_handler()

    def _get_handler(self):
        media_type = media_types_map[self.env.media_type]
        return media_type["handler"](self.env, self.filename, self.bucket, media_type["assets"])

    def ingest(self):
        return self.handler.ingest()


class LambdaHandler:
    def __init__(self):
        self.env = EnvConfig()

    def handle_event(self, event, csv_file=None):
        filename, bucket = None, None
        if event and self.env.is_lambda:
            bucket = event["Records"][0]["s3"]["bucket"]["name"]
            filename = urllib.parse.unquote_plus(
                event["Records"][0]["s3"]["object"]["key"], encoding="utf-8"
            )
        elif csv_file and not self.env.is_lambda:
            filename = csv_file

        if filename:
            media_handler = MediaTypeHandler(self.env, filename, bucket)
            return media_handler.ingest()
        else:
            raise ValueError("Filename must be provided")

    def main(self, event, context, csv_file=None):
        return self.handle_event(event, csv_file)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 lambda_function.py <filename>")
        sys.exit(1)
    else:
        filename = sys.argv[1]
        handler = LambdaHandler()
        handler.main(None, None, filename)
