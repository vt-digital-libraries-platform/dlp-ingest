from lib_files.media_types.media_type import MediaType

class IIIFType(MediaType):
  def __init__(self, env, headers_keys):
    super().__init__(env, headers_keys)