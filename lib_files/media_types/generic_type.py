
class GenericType():
  def __init__(self, env, headers_keys, metadata_filename, metadata, media_handler, metadata_handler):
    self.env = env
    self.headers_keys = headers_keys
    self.metadata_filename = metadata_filename
    self.metadata = metadata
    self.media_handler = media_handler
    self.metadata_handler = metadata_handler
  

  def ingest(self):
    if self.env["media_ingest"] == "true":
      self.import_media()
    if self.env["metadata_ingest"] == "true":
      self.import_metadata()


  def import_media(self):
    self.media_handler.import_media()


  def import_metadata(self):
    if 'manifest_list.csv' in self.metadata_filename:
        self.metadata_handler.batch_import_archives_from_legacy_manifest_list()
    elif 'collection_metadata.csv' in self.metadata_filename:
        self.metadata_handler.batch_import_collections()
    elif 'archive_metadata.csv' in self.metadata_filename:
        self.metadata_handler.batch_import_archives()