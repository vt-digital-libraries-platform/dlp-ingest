
class GenericTypeMedia():
  def __init__(self, env, headers_keys, metadata_filename, metadata):
    self.env = env
    self.headers_keys = headers_keys
    self.metadata_filename = metadata_filename
    self.metadata = metadata

  def import_media(self):
    print("import_media called from GenericTypeMedia")