from lib_files.media_types.generic_type import GenericType
from lib_files.media_types.media.three_d_type_media import ThreeDTypeMedia
from lib_files.media_types.metadata.three_d_type_metadata import ThreeDTypeMetadata

class ThreeDType(GenericType):
  def __init__(self, env, headers_keys, metadata_filename, metadata):
    self.media = ThreeDTypeMedia(env, headers_keys, metadata_filename, metadata)
    self.metadata = ThreeDTypeMetadata(env, headers_keys, metadata_filename, metadata)
    super().__init__(env, headers_keys, metadata_filename, metadata, self.media, self.metadata)
