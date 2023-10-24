from lib_files.media_types.digital_objects.generic_type_digital_objects import GenericTypeDigitalObjects

class IIIFTypeDigitalObjects(GenericTypeDigitalObjects):
  def __init__(self, env, headers_keys, metadata_filename, metadata):
    super().__init__(env, headers_keys, metadata_filename, metadata)


  def import_digital_objects(self):
    print("import_digital_objects called from IIIFTypeDigitalObjects")
