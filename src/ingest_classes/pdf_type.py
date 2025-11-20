from src.ingest_classes.generic_type import GenericType
from src.ingest_classes.digital_objects.pdf_digital_object import PDFDigitalObject
from src.ingest_classes.metadata.pdf_metadata import PDFMetadata


class PDFType(GenericType):
    def __init__(self, env, filename, bucket, assets):
        self.env = env
        self.filename = filename
        self.bucket = bucket
        self.assets = assets
        self.media = PDFDigitalObject(env, filename, bucket, assets)
        self.metadata = PDFMetadata(env, filename, bucket, assets)
        super().__init__(env, filename, bucket, self.media, self.metadata, assets)
