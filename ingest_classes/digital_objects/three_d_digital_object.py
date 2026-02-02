import boto3, os
from ingest_classes.digital_objects.generic_digital_object import GenericDigitalObject


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

    def get_bucket_paths(self, row):
        src_dir = os.path.join(
            self.env["COLLECTION_CATEGORY"],
            self.env["COLLECTION_IDENTIFIER"],
            row["identifier"],
            "3D",
            "GLB"
        )
        dest_dir = os.path.join(
            self.env["COLLECTION_CATEGORY"],
            self.env["COLLECTION_IDENTIFIER"],
            row["identifier"],
            "3d",
        )
        return src_dir, dest_dir
