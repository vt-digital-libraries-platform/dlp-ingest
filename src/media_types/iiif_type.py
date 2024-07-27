from src.media_types.generic_type import GenericType
from src.media_types.digital_objects.iiif_digital_object import IIIFDigitalObject
from src.media_types.metadata.iiif_metadata import IIIFMetadata


class IIIFType(GenericType):
    def __init__(self, env, filename, bucket, assets):
        self.env = env
        self.filename = filename
        self.bucket = bucket
        self.assets = assets
        self.media = IIIFDigitalObject(env, filename, bucket, assets)
        self.metadata = IIIFMetadata(env, filename, bucket, assets)
        super().__init__(env, filename, bucket, self.media, self.metadata, assets)
