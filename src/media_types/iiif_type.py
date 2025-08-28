import os
if os.getenv("GUI") is not None and os.getenv("GUI").lower() == "true":
    from src.dlp_ingest.src.media_types.generic_type import GenericType
    from src.dlp_ingest.src.media_types.digital_objects.iiif_digital_object import IIIFDigitalObject
    from src.dlp_ingest.src.media_types.metadata.iiif_metadata import IIIFMetadata
else:
    from src.media_types.generic_type import GenericType
    from src.media_types.digital_objects.iiif_digital_object import IIIFDigitalObject
    from src.media_types.metadata.iiif_metadata import IIIFMetadata


class IIIFType(GenericType):
    def __init__(self, env, filename, bucket, type_config):
        self.env = env
        self.filename = filename
        self.bucket = bucket
        self.type_config = type_config
        self.assets = type_config["assets"]
        self.media = IIIFDigitalObject(env, filename, bucket, type_config)
        self.metadata = IIIFMetadata(env, filename, bucket, type_config)
        super().__init__(env, filename, bucket, self.media, self.metadata, type_config)
