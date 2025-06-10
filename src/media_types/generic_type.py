from src.fixity.checksum_handler import checksum_handler

class GenericType:
    def __init__(
        self,
        env,
        filename,
        bucket,
        media_handler,
        metadata_handler,
        assets
    ):
        self.assets = assets
        self.env = env
        self.filename = filename
        self.bucket = bucket
        self.media_handler = media_handler
        self.metadata_handler = metadata_handler
        self.modified_metadata = ""

    def ingest(self):
        if self.env["media_ingest"]:
            self.modified_metadata = self.import_digital_objects()
        if self.env["metadata_ingest"]:
            self.modified_metadata = self.import_metadata()

        checksum_options = {
            "COLLECTION_IDENTIFIER": self.env["collection_identifier"],
            "FIXITY_TABLE_NAME": self.env["dynamodb_file_char_table"],
            "S3_BUCKET_NAME": self.env["aws_src_bucket"],
            "S3_PREFIX": self.env["collection_category"]
        }
        #checksum_handler(checksum_options, None)

    def import_digital_objects(self):
        return self.media_handler.import_digital_objects()

    def import_metadata(self):
        self.metadata_handler.ingest()
