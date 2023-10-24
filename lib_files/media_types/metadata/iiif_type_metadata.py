from lib_files.media_types.metadata.generic_type_metadata import GenericTypeMetadata

class IIIFTypeMetadata(GenericTypeMetadata):
  def __init__(self, env, headers_keys, metadata_filename, metadata):
    super().__init__(env, headers_keys, metadata_filename, metadata)
