from src.media_types.generic_type import GenericType
from src.media_types.digital_objects.three_d_digital_object import (
    ThreeDDigitalObject,
)
from src.media_types.metadata.three_d_metadata import ThreeDMetadata


class ThreeDType(GenericType):
    def __init__(self, env, filename, bucket, assets):
        self.assets = assets
        self.env = env
        self.filename = filename
        self.bucket = bucket
        self.media = ThreeDDigitalObject(env, filename, bucket, self.assets)
        self.metadata = ThreeDMetadata(env, filename, bucket, self.assets)
        super().__init__(env, filename, bucket, self.media, self.metadata, self.assets)
