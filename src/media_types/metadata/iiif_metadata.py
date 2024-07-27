from src.media_types.metadata.generic_metadata import GenericMetadata


class IIIFMetadata(GenericMetadata):
    def __init__(self, env, filename, bucket, assets):
        super().__init__(env, filename, bucket, assets)
