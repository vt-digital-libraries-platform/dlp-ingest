import io, json, os, urllib

if os.getenv("GUI") is not None and os.getenv("GUI").lower() == "true":
    from dlp_ingest.src.utils.s3_tools import get_matching_s3_keys
    from dlp_ingest.src.media_types.metadata.generic_metadata import GenericMetadata
else:
    from src.utils.s3_tools import get_matching_s3_keys
    from src.media_types.metadata.generic_metadata import GenericMetadata


class ThreeDMetadata(GenericMetadata):
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
                    print(f"Error: Collection not found for Archive {idx+1} in dynamo.")
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
                    print(f"Error: Collection not found for Archive {idx+1}. in env.")
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
                    archive_dict["manifest_url"] = os.path.join(
                        self.env["app_img_root_path"],
                        self.env["collection_category"],
                        collection_identifier,
                        archive_dict["identifier"],
                        "manifest.json",
                    )
                    try:
                        json_url = urllib.request.urlopen(archive_dict["manifest_url"])
                        archive_dict["thumbnail_path"] = json.loads(json_url.read())[
                            "thumbnail"
                        ]["@id"]
                    except Exception as e:
                        print(f"{archive_dict['manifest_url']} not found.")
                        archive_dict["thumbnail_path"] = None
                        archive_dict["manifest_url"] = None

                    self.archive_option_additions = self.set_archive_option_additions(
                        archive_dict
                    )
                    if archive_dict["thumbnail_path"] is None:
                        try:
                            archive_dict["thumbnail_path"] = self.archive_option_additions["assets"]["morpho_thumb"]
                        except Exception as e:
                            print(f"No thumbnail found for archive. {archive_dict['identifier']}")

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

                    try:
                        if archive_dict["archiveOptions"]["assets"]["x3d_config"] and archive_dict["archiveOptions"]["assets"]["x3d_src_img"]:
                            self.create_or_update(
                                self.env["archive_table"],
                                archive_dict,
                                "Archive",
                                idx,
                                existing_item,
                            )
                    except Exception as e:
                        print(f"Error: Archive {idx+1}:{archive_dict['identifier']} has failed to be imported.")
                        self.log_result(
                            archive_dict,
                            idx,
                            1,
                            False,
                        )
                        try:
                            print("Config")
                            print(archive_dict["archiveOptions"]["assets"]["x3d_config"])
                        except Exception as e:
                            print("Nope")
                        try:
                            print("Src Img")
                            print(archive_dict["archiveOptions"]["assets"]["x3d_src_img"])
                        except Exception as e:
                            print("Nope")

    def key_by_asset_path(self, asset_path):
        matching_key = None
        for key in get_matching_s3_keys(self.env["aws_dest_bucket"], asset_path):
            matching_key = key
        if matching_key is None:
            # try ignoring the filename case
            asset_path_no_filename = asset_path.replace(asset_path.split("/")[-1], "")
            for key in get_matching_s3_keys(
                self.env["aws_dest_bucket"], asset_path_no_filename
            ):
                if key.lower() == asset_path.lower():
                    matching_key = key
        return matching_key

    def set_archive_option_additions(self, archive_dict):
        archive_option_additions = {}
        collection_path = os.path.join(
            self.env["collection_category"],
            self.env["collection_identifier"],
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
                    asset_path = asset_path.replace(self.env["app_img_root_path"], "")
                    key = self.key_by_asset_path(asset_path)
                    if key:
                        asset_full_path = os.path.join(
                            self.env["app_img_root_path"], key
                        )
                        asset_list.append(asset_full_path)

                archive_option_additions[asset] = asset_list
            else:
                asset_path = os.path.join(
                    adjusted_path,
                    os.path.basename(asset_val).replace(
                        "<item_identifier>", archive_dict["identifier"]
                    ),
                )
                asset_path = asset_path.replace(self.env["app_img_root_path"], "")
                key = self.key_by_asset_path(asset_path)
                if key:
                    archive_option_additions[asset] = os.path.join(
                        self.env["app_img_root_path"], key
                    )
        archive_option_additions["media_type"] = self.env["media_type"]
        return {"assets": archive_option_additions}

    def create_or_update(self, table, attr_dict, item_type, idx, existing_item=None):
        if existing_item is None or len(existing_item) == 0:
            return self.create_item_in_table(table, attr_dict, item_type, idx)
        else:
            return self.update_item_in_table(table, attr_dict, existing_item, idx)

    def get_update_params(self, attr_dict, existing_item):
        append_list = [
            "format",
            "tags"
        ]
        update_expression = ["set "]
        update_values = dict()
        exp_attr_names = {
            "collection": "#col",
            "date": "#dt",
            "format": "#fmt",
            "location": "#loc",
            "references": "#ref",
            "type": "#tp",
        }
        used_keys = {}
        for key, val in attr_dict.items():
            try:
                exp_key = exp_attr_names[key]
                used_keys[exp_attr_names[key]] = key
            except KeyError:
                exp_key = key
            update_expression.append(f" {exp_key} = :{key}_val,")
            if key in append_list and type(val) == list:
                # NOTE TO SELF: This concats the two lists and removes duplicates
                update_values[f":{key}_val"] = list(set((existing_item[key] if key in existing_item else []) + val))
            else:
                update_values[f":{key}_val"] = val

        return "".join(update_expression)[:-1], update_values, used_keys

    def update_item_in_table(self, table, attr_dict, existing_item, idx):
        update_expression, update_values, used_keys = self.get_update_params(attr_dict, existing_item)
        if self.env["dry_run"]:
            print(f"Update item simulated for {existing_item['id']}")
            print(f"Update Values: {update_values}")
        else:
            print(f"Updating item {existing_item['id']}")
            print(f"Update Values: {update_values}")
            try:
                response = table.update_item(
                    Key={"id": existing_item["id"]},
                    UpdateExpression=update_expression,
                    ExpressionAttributeValues=update_values,
                    ExpressionAttributeNames=used_keys,
                    ReturnValues="UPDATED_NEW",
                )

                self.log_result(
                    response["Attributes"],
                    idx,
                    2,
                    True,
                )
            except Exception as e:
                print(e)
                self.log_result(
                    attr_dict,
                    idx,
                    2,
                    False,
                )
