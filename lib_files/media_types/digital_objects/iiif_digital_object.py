from lib_files.media_types.digital_objects.generic_digital_object import (
    GenericDigitalObject,
)


class IIIFDigitalObject(GenericDigitalObject):
    def __init__(self, env, filename, bucket, assets):
        self.env = env
        self.filename = filename
        self.bucket = bucket
        self.assets = assets
        super().__init__(env, filename, bucket, assets)

    def import_digital_objects(self):
        print("import_digital_objects called from IIIFTypeDigitalObjects")
        return None
