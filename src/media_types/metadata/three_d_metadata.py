import http, io, json, os, urllib
from src.s3_tools import get_matching_s3_keys
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
            print("===================================")
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
                    except (
                        urllib.error.HTTPError,
                        http.client.IncompleteRead,
                    ) as http_err:
                        print(http_err)
                        print(f"{archive_dict['manifest_url']} not found.")
                        self.log_result(
                            archive_dict,
                            idx,
                            4,
                            False,
                        )
                    except Exception as e:
                        print(e)
                        print(f"{archive_dict['manifest_url']} not found.")
                        self.log_result(
                            archive_dict,
                            idx,
                            4,
                            False,
                        )
                    self.archive_option_additions = self.set_archive_option_additions(
                        archive_dict
                    )

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

                    if (
                        "thumbnail_path" in archive_dict
                        and len(archive_dict["thumbnail_path"]) > 0
                    ):
                        self.create_or_update(
                            self.env["archive_table"],
                            archive_dict,
                            "Archive",
                            idx,
                            existing_item,
                        )

    def key_by_asset_path(self, asset_path):
        matching_key = None
        print(f"Looking for key that matches: {asset_path}")
        for key in get_matching_s3_keys(self.env["aws_dest_bucket"], asset_path):
            matching_key = key
            print(f"Key: {key}")
            print(f"Match")
        print("===================================")
        if matching_key is None:
            # try ignoring the filename case
            print("No match found, trying to ignore filename case")
            asset_path_no_filename = asset_path.replace(asset_path.split("/")[-1], "")
            for key in get_matching_s3_keys(
                self.env["aws_dest_bucket"], asset_path_no_filename
            ):
                if key.lower() == asset_path.lower():
                    matching_key = key
                    print(f"Key: {key}")
                    print(f"Match")
            if matching_key is None:
                print("No match found still? I got nothing.")
        print("===================================")
        return matching_key

    def set_archive_option_additions(self, archive_dict):
        archive_option_additions = {}
        collection_path = os.path.join(
            self.env["collection_category"],
            self.env["collection_identifier"],
        )
        archive_3d_asset_path = os.path.join(
            collection_path, archive_dict["identifier"], "3d"
        )
        for asset in self.assets["item"]:
            if type(self.assets["item"][asset]) == list:
                asset_list = []
                for item_asset in self.assets["item"][asset]:
                    asset_path = os.path.join(
                        archive_3d_asset_path,
                        item_asset.split("/")[-1].replace(
                            "<item_identifier>", archive_dict["identifier"]
                        ),
                    )
                    asset_path = asset_path.replace(self.env["app_img_root_path"], "")
                    print(f"Asset Path: {asset_path}")
                    print(self.env["app_img_root_path"])
                    key = self.key_by_asset_path(asset_path)
                    if key:
                        asset_full_path = os.path.join(
                            self.env["app_img_root_path"], key
                        )
                        asset_list.append(asset_full_path)

                archive_option_additions[asset] = asset_list
            else:
                asset_path = os.path.join(
                    archive_3d_asset_path,
                    self.assets["item"][asset]
                    .split("/")[-1]
                    .replace("<item_identifier>", archive_dict["identifier"]),
                )
                asset_path = asset_path.replace(self.env["app_img_root_path"], "")
                print(f"Asset Path: {asset_path}")
                print(self.env["app_img_root_path"])
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

    def get_update_params(self, body):
        update_expression = ["set "]
        update_values = dict()
        exp_attr_names = {
            "collection": "#col",
            "date": "#dt",
            "format": "#fmt",
            "location": "#loc",
        }
        used_keys = {}
        for key, val in body.items():
            try:
                exp_key = exp_attr_names[key]
                used_keys[exp_attr_names[key]] = key
            except KeyError:
                exp_key = key
            update_expression.append(f" {exp_key} = :{key}_val,")
            update_values[f":{key}_val"] = val

        return "".join(update_expression)[:-1], update_values, used_keys

    def update_item_in_table(self, table, attr_dict, existing_item, idx):
        update_expression, update_values, used_keys = self.get_update_params(attr_dict)
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
