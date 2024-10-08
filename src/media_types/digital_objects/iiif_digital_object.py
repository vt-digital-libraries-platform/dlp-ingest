import boto3
from src.media_types.digital_objects.generic_digital_object import (
    GenericDigitalObject,
)


class IIIFDigitalObject(GenericDigitalObject):
    def __init__(self, env, filename, bucket, assets):
        self.env = env
        self.filename = filename
        self.bucket = bucket
        self.assets = assets
        self.s3_client = boto3.client("s3")
        self.s3_resource = boto3.resource("s3")
        super().__init__(
            env, filename, bucket, assets, self.s3_client, self.s3_resource
        )

    def import_digital_objects(self):
        print("import_digital_objects called from IIIFTypeDigitalObjects")
        return None
