import os
from ingest_classes.metadata.generic_metadata import GenericMetadata
from ingest_classes.metadata.iiif_manifest_mixin import IIIFManifestMixin


class IIIFMetadata(IIIFManifestMixin, GenericMetadata):
    def apply_collection_to_archive(self, archive_dict, collection):
        archive_dict["collection"] = collection["id"]
        archive_dict["parent_collection"] = [collection["id"]]
        archive_dict["parent_collection_identifier"] = [collection["identifier"]]
        archive_dict["heirarchy_path"] = collection["heirarchy_path"]
        archive_dict["manifest_url"] = os.path.join(
            self.env["APP_IMG_ROOT_PATH"],
            self.env["COLLECTION_CATEGORY"],
            collection["identifier"],
            archive_dict["identifier"],
            "manifest.json",
        )
        archive_dict["thumbnail_path"] = self.get_thumbnail_path_for_iiif(archive_dict)

        # if you can't find the thumbnail for an item, skip it, because that means the manifest couldn't be found or read
        if "thumbnail_path" not in archive_dict or not archive_dict["thumbnail_path"]:
            self.logger.warning(f"Could not find or read the manifest for Item {archive_dict['identifier']}")
            self.logger.warning(f"Looked here for the manifest: {archive_dict['manifest_url']}")
            self.logger.warning(f"Skipping this record.")
            return False

        return True
