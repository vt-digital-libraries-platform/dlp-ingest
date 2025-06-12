import os
if os.getenv("GUI") is not None and os.getenv("GUI").lower() == "true":
    from dlp_ingest.src.media_types.metadata.generic_metadata import GenericMetadata
else:
    from src.media_types.metadata.generic_metadata import GenericMetadata


class IIIFMetadata(GenericMetadata):
    def __init__(self, env, filename, bucket, assets):
        super().__init__(env, filename, bucket, assets)
