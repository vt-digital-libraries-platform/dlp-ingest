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

    def ingest(self):
        if self.env["media_ingest"]:
            self.modified_metadata = self.import_digital_objects()
        if self.env["metadata_ingest"]:
            self.import_metadata()

    def import_digital_objects(self):
        return self.media_handler.import_digital_objects()

    def import_metadata(self):
        self.metadata_handler.ingest()
