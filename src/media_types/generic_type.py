import os

if os.getenv("GUI") is not None and os.getenv("GUI").lower() == "true":
    from src.dlp_ingest.src.fixity.checksum_handler import checksum_handler
else:
    from src.fixity.checksum_handler import checksum_handler

class GenericType:
    def __init__(
        self,
        env,
        filename,
        bucket,
        media_handler,
        metadata_handler,
        type_config
    ):
        self.type_config = type_config
        self.assets = type_config["assets"]
        self.env = env
        self.filename = filename
        self.bucket = bucket
        self.media_handler = media_handler
        self.metadata_handler = metadata_handler
        self.modified_metadata = ""

    def ingest(self):
        if self.env["MEDIA_INGEST"]:
            self.modified_metadata = self.import_digital_objects()
        if self.env["METADATA_INGEST"]:
            self.modified_metadata = self.import_metadata()

        checksum_options = {
            "COLLECTION_IDENTIFIER": self.env["COLLECTION_IDENTIFIER"],
            "FIXITY_TABLE_NAME": self.env["DYNAMODB_FILE_CHAR_TABLE"],
            "S3_BUCKET_NAME": self.env["AWS_SRC_BUCKET"],
            "S3_PREFIX": self.env["COLLECTION_CATEGORY"]
        }
        checksum_handler(checksum_options, None)

    def import_digital_objects(self):
        return self.media_handler.import_digital_objects()

    def import_metadata(self):
        self.metadata_handler.ingest()
