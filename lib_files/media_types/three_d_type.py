from lib_files.media_types.generic_type import GenericType
from lib_files.media_types.digital_objects.three_d_type_digital_objects import ThreeDTypeDigitalObjects
from lib_files.media_types.metadata.three_d_type_metadata import ThreeDTypeMetadata

class ThreeDType(GenericType):
  def __init__(self, env, headers_keys, metadata_filename, metadata):
    self.media = ThreeDTypeDigitalObjects(env, headers_keys, metadata_filename, metadata)
    self.metadata = ThreeDTypeMetadata(env, headers_keys, metadata_filename, metadata)
    super().__init__(env, headers_keys, metadata_filename, metadata, self.media, self.metadata)
