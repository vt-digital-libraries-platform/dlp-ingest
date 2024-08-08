import boto3
from src.media_types.digital_objects.generic_digital_object import (
    GenericDigitalObject,
)


class ThreeDDigitalObject(GenericDigitalObject):
    def __init__(self, env, filename, bucket, assets):
        self.assets = assets
        self.env = env
        self.filename = filename
        self.bucket = bucket
        self.s3_client = boto3.client("s3")
        self.s3_resource = boto3.resource("s3")
        super().__init__(
            env, filename, bucket, assets, self.s3_client, self.s3_resource
        )
