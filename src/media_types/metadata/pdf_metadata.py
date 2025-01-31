import http, io, json, os, urllib
from src.s3_tools import get_matching_s3_keys
from src.media_types.metadata.generic_metadata import GenericMetadata


class PDFMetadata(GenericMetadata):
    def __init__(self, env, filename, bucket, assets):
        self.assets = assets
        self.env = env
        self.filename = filename
        self.bucket = bucket
        self.archive_option_additions = {}
        super().__init__(self.env, self.filename, self.bucket, self.assets)

    def batch_import_archives(self, response):
        df = self.csv_to_dataframe(io.BytesIO(response["Body"].read()))
        for idx, row in df.iterrows():
            print("")
            archive_dict = self.process_csv_metadata(row, "Archive")
            if not archive_dict:
                print(f"Error: Archive {idx+1} has failed to be imported.")
                self.log_result(
                    False,
                    idx,
                    1,
                    False,
                )
                break
            else:
                collection = self.get_collection(archive_dict)
                if not collection:
                    print(f"Error: Collection not found for Archive {idx+1}.")
                    print("Error: Archive must belong to a collection to be ingested")
                    self.log_result(
                        archive_dict,
                        idx,
                        3,
                        False,
                    )
                    break
                collection_identifier = (
                    collection["identifier"]
                    if collection
                    else self.env["collection_identifier"]
                )
                if collection_identifier is None:
                    print(f"Error: Collection not found for Archive {idx+1}.")
                    print("Error: Archive must belong to a collection to be ingested")
                    self.log_result(
                        archive_dict,
                        idx,
                        3,
                        False,
                    )
                    break
                else:
                    archive_dict["collection"] = collection["id"]
                    archive_dict["parent_collection"] = [collection["id"]]
                    archive_dict["heirarchy_path"] = collection["heirarchy_path"]
                    archive_dict["manifest_url"] = self.asset_path(
                        archive_dict, collection_identifier
                    )
                    archive_dict["thumbnail_path"] = self.asset_path(
                        archive_dict, collection_identifier, "thumbnail"
                    )
                    self.archive_option_additions = self.set_archive_option_additions(
                        archive_dict
                    )
                    if (
                        "thumbnail_path" in archive_dict
                        and len(archive_dict["thumbnail_path"]) > 0
                    ):
                        self.create_if_not_exists(
                            self.env["archive_table"], archive_dict, "Archive", idx
                        )
                    else:
                        print(archive_dict["thumbnail_path"] or f"No thumbnail path for {archive_dict['identifier']}")

    def asset_path(self, archive_dict, collection_identifier, asset_type=None):
        if asset_type is None:
            asset_type = self.assets["options"]["asset_src"]
        asset_url = ""
        try:
            prefix = os.path.join(
                self.env["collection_category"],
                collection_identifier,
                archive_dict["identifier"],
            )
            suffix = self.assets["item"][asset_type].replace("<variable>", "")
        except Exception as e:
            print(e)
            return ""
        asset_url = ""
        for key in get_matching_s3_keys(self.env["aws_dest_bucket"], prefix, suffix):
            asset_url = os.path.join(self.env["app_img_root_path"], key)
        return asset_url

    def key_by_asset_path(self, asset_path):
        matching_key = None
        for key in get_matching_s3_keys(self.env["aws_dest_bucket"], asset_path):
            matching_key = key
            print(f"Key: {key}")
            print(f"Match")
        if matching_key is None:
            # try ignoring the filename case
            asset_path_no_filename = asset_path.replace(asset_path.split("/")[-1], "")
            for key in get_matching_s3_keys(
                self.env["aws_dest_bucket"], asset_path_no_filename
            ):
                if key.lower() == asset_path.lower():
                    matching_key = key
                    print(f"Key: {key}")
                    print(f"Match")
            if matching_key is None:
                print(f"Couldn't find key for: {asset_path}.")

        return matching_key

    def set_archive_option_additions(self, archive_dict):
        archive_option_additions = {}
        archive_option_additions["media_type"] = self.env["media_type"]
        return {"assets": archive_option_additions}
