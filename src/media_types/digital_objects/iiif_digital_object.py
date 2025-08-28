import boto3, os
if os.getenv("GUI") is not None and os.getenv("GUI").lower() == "true":
    from src.dlp_ingest.src.media_types.digital_objects.generic_digital_object import GenericDigitalObject
else:
    from src.media_types.digital_objects.generic_digital_object import GenericDigitalObject


class IIIFDigitalObject(GenericDigitalObject):
    def __init__(self, env, filename, bucket, type_config):
        self.env = env
        self.filename = filename
        self.bucket = bucket
        self.type_config = type_config
        self.assets = type_config["assets"]
        self.s3_client = boto3.client("s3")
        self.s3_resource = boto3.resource("s3")
        super().__init__(
            env, filename, bucket, type_config, self.s3_client, self.s3_resource
        )

    def import_digital_objects(self):
        print("import_digital_objects called from IIIFTypeDigitalObjects")
        return None
