from lib_files.media_types.metadata.generic_type_metadata import GenericTypeMetadata

class ThreeDTypeMetadata(GenericTypeMetadata):
  def __init__(self, env, headers_keys, metadata_filename, metadata):
    super().__init__(env, headers_keys, metadata_filename, metadata)


  def batch_import_archives(self):
    print("batch_import_archives called from ThreeDTypeMetadata")

