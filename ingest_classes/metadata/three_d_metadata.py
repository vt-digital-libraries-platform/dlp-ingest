import io, json, logging, os, urllib
from utils.s3_tools import get_matching_s3_keys
from ingest_classes.metadata.generic_metadata import GenericMetadata


class ThreeDMetadata(GenericMetadata):
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
            self.logger.info(f"archive: {archive_dict}")
            if not archive_dict:
                self.logger.error(f"Error: reading item on line {idx+1} from csv.")
                continue
            else:
                collection = self.get_collection(archive_dict)
                self.logger.info(f"collection: {collection}")
                if not collection:
                    self.logger.error(f"Error: Collection not found in dynamo for item at row {idx+1}.")
                    continue

                collection_identifier = collection["identifier"] if collection else self.env["COLLECTION_IDENTIFIER"]
                if collection_identifier is None:
                    self.logger.error(f"Error: Collection not found for Archive {idx+1}. in env.")
                    continue
                else:
                    archive_dict["collection"] = collection["id"]
                    archive_dict["parent_collection"] = [collection["id"]]
                    archive_dict["heirarchy_path"] = collection["heirarchy_path"]
                    
                    # try to load iiif manifest and get thumbnail, in case it's a 3d + iiif record
                    archive_dict["manifest_url"] = os.path.join(
                        self.env["APP_IMG_ROOT_PATH"],
                        self.env["COLLECTION_CATEGORY"],
                        collection_identifier,
                        archive_dict["identifier"],
                        "manifest.json",
                    )

                    archive_dict["thumbnail_path"] = self.get_thumbnail_path_for_archive(archive_dict)
                        

                    # set archive options
                    self.archive_option_additions = self.set_archive_options(
                        archive_dict
                    )
                    if archive_dict["thumbnail_path"] is None:
                        try:
                            archive_dict["thumbnail_path"] = self.archive_option_additions["assets"]["morpho_thumb"]
                        except Exception as e:
                            self.logger.error(f"Unable to set thumbnail_path for archive: {archive_dict["identifier"]}")

                    existing_item = self.query_by_index(
                        self.env["archive_table"],
                        "Identifier",
                        archive_dict["identifier"],
                    )
                    if existing_item is not None and "archiveOptions" in existing_item:
                        archive_dict["archiveOptions"] = {
                            **existing_item["archiveOptions"],
                            **self.archive_option_additions,
                        }
                    else:
                        archive_dict["archiveOptions"] = self.archive_option_additions
                    
                    existing_archive = self.query_by_index(self.env["archive_table"], "Identifier", archive_dict["identifier"])
                    if existing_archive:
                        if self.env["UPDATE_METADATA"]:
                            self.update_item_in_table(self.env["archive_table"], existing_archive["id"], archive_dict, archive_dict["identifier"])
                        else:
                            continue
                    else:
                        self.logger.info(f"before save {archive_dict}")
                        self.create_item_in_table(self.env["archive_table"], archive_dict, "Archive")



    def key_by_asset_path(self, asset_path):
        matching_key = None
        keys = None
        if self.env["AWS_DEST_BUCKET"] is None or asset_path is None:
            return
        try:
            keys = get_matching_s3_keys(self.env["AWS_DEST_BUCKET"], asset_path)
        except:
            pass
        if keys is None:
            return
        for key in keys:
            matching_key = key
        if matching_key is None:
            # try ignoring the filename case
            asset_path_no_filename = asset_path.replace(asset_path.split("/")[-1], "")
            for key in get_matching_s3_keys(
                self.env["AWS_DEST_BUCKET"], asset_path_no_filename
            ):
                if key.lower() == asset_path.lower():
                    matching_key = key
        return matching_key


    def set_archive_options(self, archive_dict):
        archive_assets = {}
        archive_config = {}
        collection_path = os.path.join(
            self.env["COLLECTION_CATEGORY"],
            self.env["COLLECTION_IDENTIFIER"],
        )
        archive_asset_path = os.path.join(collection_path, archive_dict["identifier"])
        archive_3d_asset_path = os.path.join(archive_asset_path, "3d")
        for asset in self.assets["item"]:
            adjusted_path = (
                archive_asset_path
                if asset == "iiif_manifest"
                else archive_3d_asset_path
            )
            asset_val = self.assets["item"][asset]
            if type(asset_val) == list:
                asset_list = []
                for item_asset in asset_val:
                    asset_path = os.path.join(
                        adjusted_path,
                        os.path.basename(item_asset).replace(
                            "<item_identifier>", archive_dict["identifier"]
                        ),
                    )
                    asset_path = asset_path.replace(self.env["APP_IMG_ROOT_PATH"], "")
                    if asset_path:
                        key = self.key_by_asset_path(asset_path)
                    if key:
                        asset_full_path = os.path.join(
                            self.env["APP_IMG_ROOT_PATH"], key
                        )
                        asset_list.append(asset_full_path)

                archive_assets[asset] = asset_list
            else:
                asset_path = os.path.join(
                    adjusted_path,
                    os.path.basename(asset_val).replace(
                        "<item_identifier>", archive_dict["identifier"]
                    ),
                )
                asset_path = asset_path.replace(self.env["APP_IMG_ROOT_PATH"], "")
                if asset_path:
                    key = self.key_by_asset_path(asset_path)
                if key:
                    archive_assets[asset] = os.path.join(
                        self.env["APP_IMG_ROOT_PATH"], key
                    )
                archive_assets["env_config"] = os.path.join(
                        self.env["APP_IMG_ROOT_PATH"], "federated/3d/gltf/studio.env"
                    )
                archive_assets["scale_factor"] = "75"

        archive_assets["media_type"] = "3d-model/gltf"

        # start config
        archive_config["_3d"] = {}
        archive_config["_3d"]["rotation"] = {}
        
        if "3D_OPTIONS-ROTATION-X" in self.env:
            archive_config["_3d"]["rotation"]["x"] = self.env["3D_OPTIONS-ROTATION-X"]
        if "3D_OPTIONS-ROTATION-Y" in self.env:
            archive_config["_3d"]["rotation"]["y"] = self.env["3D_OPTIONS-ROTATION-Y"]

        if "3D_OPTIONS-SCALE" in self.env:
            archive_config["_3d"]["scale_factor"] = self.env["3D_OPTIONS-SCALE"]

        if "3D_OPTIONS-ADDONS" in self.env and self.env["3D_OPTIONS-ADDONS"]:
            archive_config["_3d"]["addOns"] = []
            if self.env["3D_OPTIONS-ADDONS"] == "flash_card":
                flash_card = {}
                flash_card["type"] = "flash_card"
                if "3D_OPTIONS-FLASH_CARD-OPTIONS-TEXT-BACK" in self.env:
                    flash_card["back"] = {
                        "type": "metadata",
                        "value": self.env["3D_OPTIONS-FLASH_CARD-OPTIONS-TEXT-BACK"]
                    }
                if "3D_OPTIONS-FLASH_CARD-OPTIONS-TEXT-FRONT" in self.env:
                    flash_card["front"] = {
                        "type": "string",
                        "value": self.env["3D_OPTIONS-FLASH_CARD-OPTIONS-TEXT-FRONT"]
                    }
                archive_config["_3d"]["addOns"].append(flash_card)
 
        return {"assets": archive_assets, "config": archive_config}

