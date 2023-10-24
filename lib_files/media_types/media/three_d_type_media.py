from lib_files.media_types.media.generic_type_media import GenericTypeMedia

class ThreeDTypeMedia(GenericTypeMedia):
  def __init__(self, env, headers_keys, metadata_filename, metadata):
    super().__init__(env, headers_keys, metadata_filename, metadata)


  def import_media(self):
    print("import_media called from ThreeDTypeMedia")
