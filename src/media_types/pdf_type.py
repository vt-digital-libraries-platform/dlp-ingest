from src.media_types.generic_type import GenericType
from src.media_types.digital_objects.pdf_digital_object import PDFDigitalObject
from src.media_types.metadata.pdf_metadata import PDFMetadata


class PDFType(GenericType):
    def __init__(self, env, filename, bucket, assets):
        self.env = env
        self.filename = filename
        self.bucket = bucket
        self.assets = assets
        self.media = PDFDigitalObject(env, filename, bucket, assets)
        self.metadata = PDFMetadata(env, filename, bucket, assets)
        super().__init__(env, filename, bucket, self.media, self.metadata, assets)
