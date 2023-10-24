from lib_files.media_types.generic_type import GenericType
from lib_files.media_types.digital_objects.iiif_type_digital_objects import IIIFTypeDigitalObjects
from lib_files.media_types.metadata.iiif_type_metadata import IIIFTypeMetadata

class IIIFType(GenericType):
  def __init__(self, env, headers_keys, metadata_filename, metadata):
    self.media = IIIFTypeDigitalObjects(env, headers_keys, metadata_filename, metadata)
    self.metadata = IIIFTypeMetadata(env, headers_keys, metadata_filename, metadata)
    super().__init__(env, headers_keys, metadata_filename, metadata, self.media, self.metadata)