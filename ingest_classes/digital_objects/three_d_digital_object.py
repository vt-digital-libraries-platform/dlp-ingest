import os
from ingest_classes.digital_objects.generic_digital_object import GenericDigitalObject


class ThreeDDigitalObject(GenericDigitalObject):
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
