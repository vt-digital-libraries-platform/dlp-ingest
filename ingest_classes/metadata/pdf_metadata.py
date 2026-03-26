import io, logging, os
from utils.s3_tools import get_matching_s3_keys
from ingest_classes.metadata.generic_metadata import GenericMetadata


class PDFMetadata(GenericMetadata):
    def __init__(self, env, filename, bucket, assets):
        self.assets = assets
        self.env = env
        self.filename = filename
        self.bucket = bucket
        self.archive_option_additions = {}
        self.logger = logging.getLogger()
        super().__init__(self.env, self.filename, self.bucket, self.assets)

    def batch_import_archives(self, response):
        df = self.csv_to_dataframe(io.BytesIO(response["Body"].read()))
        for idx, row in df.iterrows():
            archive_dict = self.process_csv_metadata(row, "Archive")
            if not archive_dict:
                self.logger.error(f"Error: Archive {idx+1} has failed to be imported.")
                continue
            else:
                dates_valid = self.validate_archive_dates(archive_dict)
                if not dates_valid:
                    self.logger.error(f"Error: Archive {archive_dict.get('identifier', 'N/A')} has invalid date formats. Skipping this record.")
                    continue
                collection = self.get_collection(archive_dict)
                if not collection:
                    self.logger.error(f"Error: Collection not found for Archive {idx+1}.")
                    self.logger.error("Error: Archive must belong to a collection to be ingested")
                    continue
                collection_identifier = (
                    collection["identifier"]
                    if collection
                    else self.env["COLLECTION_IDENTIFIER"]
                )
                if collection_identifier is None:
                    self.logger.error(f"Error: Collection not found for Archive {idx+1}.")
                    self.logger.error("Error: Archive must belong to a collection to be ingested")
                    continue
                else:
                    archive_dict["collection"] = collection["id"]
                    archive_dict["parent_collection"] = [collection["id"]]
                    archive_dict["heirarchy_path"] = collection["heirarchy_path"]
                    archive_dict["manifest_url"] = self.asset_path(
                        archive_dict, collection_identifier, "pdf"
                    )
                    self.logger.info(f"pdf: {archive_dict['manifest_url']}")
                    archive_dict["thumbnail_path"] = self.asset_path(
                        archive_dict, collection_identifier, "thumbnail"
                    )
                    self.archive_option_additions = self.set_archive_option_additions()

                    existing_archive = self.query_by_index(self.env["archive_table"], "Identifier", archive_dict["identifier"])
                    if existing_archive:
                        if self.env["UPDATE_METADATA"]:
                            self.update_item_in_table(self.env["archive_table"], existing_archive["id"], archive_dict, archive_dict["identifier"])
                        else:
                            continue
                    else:
                        self.create_item_in_table(self.env["archive_table"], archive_dict, "Archive")


    def asset_path(self, archive_dict, collection_identifier, asset_type=None):
        if asset_type is None:
            asset_type = self.assets["options"]["asset_src"]
        self.logger.info(f"asset type: {asset_type}")
        asset_url = ""
        try:
            prefix = os.path.join(
                self.env["COLLECTION_CATEGORY"],
                collection_identifier,
                archive_dict["identifier"],
            )
            suffix = self.assets["item"][asset_type].replace("<item_identifier>", archive_dict["identifier"])
            self.logger.info(f"prefix: {prefix}")
            self.logger.info(f"suffix: {suffix}")
        except Exception as e:
            self.logger.error(e)
            return ""
        matches = get_matching_s3_keys(self.env["AWS_DEST_BUCKET"], prefix, suffix)
        if matches:
            for key in matches:
                asset_url = os.path.join(self.env["APP_IMG_ROOT_PATH"], key)
        self.logger.info(f"asset_url: {asset_url}")
        return asset_url


    def key_by_asset_path(self, asset_path):
        matching_key = None
        matches = get_matching_s3_keys(self.env["AWS_DEST_BUCKET"], asset_path)
        if matches:
            for key in matches:
                matching_key = key
            if matching_key is None:
                # try ignoring the filename case
                asset_path_no_filename = asset_path.replace(asset_path.split("/")[-1], "")
                matches = get_matching_s3_keys(self.env["AWS_DEST_BUCKET"], asset_path_no_filename)
                if matches:
                    for key in matches:
                        if key.lower() == asset_path.lower():
                            matching_key = key
                    if matching_key is None:
                        self.logger.error(f"Couldn't find key for: {asset_path}.")
        return matching_key


    def set_archive_option_additions(self):
        archive_option_additions = {}
        archive_option_additions["media_type"] = self.env["MEDIA_TYPE"]
        return {"assets": archive_option_additions}
