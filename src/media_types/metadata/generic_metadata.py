import boto3, http, io, json, os, uuid, urllib.request
import pandas as pd
from datetime import datetime, timezone
from dateutil.parser import parse
from boto3.dynamodb.conditions import Key, Attr
from botocore.response import StreamingBody
import re

DUPLICATED = "Duplicated"

class GenericMetadata:
    def __init__(self, env, filename, bucket, assets):
        self.assets = assets
        self.env = env
        self.filename = filename
        self.bucket = bucket
        self.single_value_headers = None
        self.multi_value_headers = None
        self.results = []

        try:
            self.dyndb = boto3.resource("dynamodb", region_name=self.env["region_name"])
            self.env["archive_table"] = self.dyndb.Table(self.get_table_name("Archive"))
            self.env["collection_table"] = self.dyndb.Table(
                self.get_table_name("Collection")
            )
            self.env["collectionmap_table"] = self.dyndb.Table(
                self.get_table_name("Collectionmap")
            )
            self.env["mint_table"] = self.dyndb.Table(self.env["dynamodb_noid_table"])
        except Exception as e:
            print(f"An error occurred connecting to an AWS Dynamo resource: {str(e)}")
            raise e

        try:
            self.env["s3_resource"] = boto3.resource("s3")
            self.env["s3_client"] = boto3.client("s3")
        except Exception as e:
            print(f"An error occurred connecting to an AWS s3 resource: {str(e)}")
            raise e

        try:
            headers_file = None
            if os.getenv("GUI") is not None and os.getenv("GUI").lower() == "true":
                print(os.getcwd())
                headers_file = "./dlp_ingest/src/data/20240702_headers_keys.json"
            else:
                headers_file = "./src/data/20240702_headers_keys.json"
            
            headers_json = os.path.join(headers_file)
            with open(headers_json) as f:
                headers_keys = json.load(f)
                self.single_value_headers = headers_keys["single_value_headers"]
                self.multi_value_headers = headers_keys["multi_value_headers"]
        except Exception as e:
            print(f"An error occurred reading headers_keys.json: {str(e)}")
            raise e

    def ingest(self):
        metadata_stream = None
        if "is_lambda" in self.env and self.env["is_lambda"]:
            metadata_stream = self.lambda_metadata(self.filename, self.bucket)
        else:
            metadata_stream = self.local_metadata(self.filename)

        if "_collection_metadata.csv" in self.filename:
            self.batch_import_collections(metadata_stream)
        elif "_archive_metadata.csv" in self.filename or "_item_metadata.csv" in self.filename:
            self.batch_import_archives(metadata_stream)
        else:
            error_message = f"Error: {self.filename} is not a valid filename."
            print(error_message)
            print(
                "Filenames must end with [ _collection_metadata.csv | _archive_metadata.csv ] in order to be processed."
            )
            return {"statusCode": 200, "body": json.dumps(error_message)}

        print("Results: ===================================================")
        print("Total records processed: ", len(self.results))
        self.print_results()
        return {"statusCode": 200, "body": json.dumps("Finish metadata import.")}

    def local_metadata(self, filename):
        body_encoded = open(filename).read().encode()
        stream = StreamingBody(io.BytesIO(body_encoded), len(body_encoded))
        return {"Body": stream}

    def lambda_metadata(self, key, bucket):
        try:
            return self.env["s3_client"].get_object(Bucket=bucket, Key=key)
        except Exception as e:
            print(f"An error occurred reading {key} from {bucket}: {str(e)}")
        return None

    def batch_import_collections(self, response):
        df = self.csv_to_dataframe(io.BytesIO(response["Body"].read()))
        for idx, row in df.iterrows():
            print("")
            print("===================================")
            collection_dict = self.process_csv_metadata(row, "Collection")
            if not collection_dict:
                print(f"Error: Collection {idx+1} has failed to be imported.")
                self.log_result(
                    False,
                    idx,
                    1,
                    False,
                )
                break
            identifier = collection_dict["identifier"]
            if "thumbnail_path" not in collection_dict or not collection_dict["thumbnail_path"]:
                collection_dict["thumbnail_path"] = os.path.join(
                    self.env["app_img_root_path"],
                    self.env["collection_category"],
                    identifier,
                    "representative.jpg",
                )
            self.create(
                self.env["collection_table"], collection_dict, "Collection", idx
            )
            #Revisit the collection map update logic
            if "heirarchy_path" in collection_dict:
                self.update_collection_map(collection_dict["heirarchy_path"][0])
            print(f"Collection {idx+1} ({identifier}) has been imported.")

    def batch_import_archives(self, response):
        df = self.csv_to_dataframe(io.BytesIO(response["Body"].read()))
        for idx, row in df.iterrows():
            print()
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

                    if (
                        "thumbnail_path" in archive_dict
                        and len(archive_dict["thumbnail_path"]) > 0
                    ):
                        self.create(
                                self.env["archive_table"],
                                archive_dict,
                                "Archive",
                                idx
                            )
                            # Log trying to create an item that already exists

    def get_table_name(self, table_name):
        return f"{table_name}-{self.env['dynamodb_table_suffix']}"

    def header_update(self, records):
        df = records.rename(
            columns={
                "dcterms.alternative": "alternative",
                "dcterms.bibliographicCitation": "bibliographic_citation",
                "dcterms.conformsTo": "conforms_to",
                "dcterms.contributor": "contributor",
                "dcterms.coverage": "coverage",
                "dcterms.created": "created",
                "dcterms.creator": "creator",
                "dcterms.date": "date",
                "dcterms.description": "description",
                "dcterms.extent": "extent",
                "dcterms.format": "format",
                "dcterms.hasFormat": "has_format",
                "dcterms.hasPart": "has_part",
                "dcterms.hasVersion": "has_version",
                "dcterms.identifier": "identifier",
                "dcterms.isFormatOf": "is_format_of",
                "dcterms.isPartOf": "belongs_to",
                "dcterms.isVersionOf": "is_version_of",
                "dcterms.language": "language",
                "dcterms.license": "license",
                "dcterms.medium": "medium",
                "dcterms.provenance": "provenance",
                "dcterms.publisher": "publisher",
                "dcterms.references": "reference",
                "dcterms.relation": "related_url",
                "dcterms.rights": "rights_statement",
                "dcterms.source": "source",
                "dcterms.spatial": "location",
                "dcterms.subject": "subject",
                "dcterms.rightsHolder": "rights_holder",
                "dcterms.temporal": "temporal",
                "dcterms.title": "title",
                "dcterms.type": "resource_type",
            }
        )
        return df

    def csv_to_dataframe(self, csv_path):
        df = pd.read_csv(
            csv_path,
            na_values="NaN",
            keep_default_na=False,
            encoding="utf-8",
            dtype={"Start Date": str, "End Date": str},
        )
        df = self.header_update(df)
        return df

    def status_message(self, msg_index, csv_index, attr_dict, succeeded):
        messages = {
            0: f"Row {csv_index}: {attr_dict['identifier']} has been successfully imported. Succeeded: {str(succeeded)}",
            1: f"Error Row {csv_index}: Invalid {attr_dict['identifier']} record defined in row. Succeeded: {str(succeeded)}",
            2: f"Error Row {csv_index}: Identifier ({attr_dict['identifier']}) already exists in Dynamo table. Succeeded: {str(succeeded)}",
            3: f"Error Row {csv_index}: Collection record not found for Archive {attr_dict['identifier']}. Succeeded: {str(succeeded)}",
            4: f"Error Row {csv_index}: manifest.json not found for Archive {attr_dict['identifier']} in s3 bucket. Succeeded: {str(succeeded)}",
        }
        return messages.get(msg_index)

    def log_result(self, attr_dict, csv_index, message_index, succeeded=True):
        identifier = (
            attr_dict["identifier"]
            if (attr_dict and "identifier" in attr_dict)
            else "N/A"
        )
        self.results.append(
            {
                "row_in_metadata": csv_index + 2,
                "identifier": identifier,
                "succeeded": succeeded,
                "message": self.status_message(
                    message_index, csv_index + 2, attr_dict, succeeded
                ),
            }
        )

    def print_results(self):
        df = pd.DataFrame(self.results)
        results_filename = f"{self.env['collection_identifier']}_ingest_results_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
        working_dir = os.path.abspath(os.path.dirname(__file__))
        results_path = os.path.join(working_dir, "results_files")
        if not os.path.exists(results_path):
            os.mkdir(results_path)
        os.chdir(results_path)
        df.to_csv(results_filename, index=False)
        s3_response = None
        target = os.path.join(
            self.env["collection_category"],
            self.env["collection_identifier"],
            "metadata_import_results",
            results_filename,
        )
        if os.path.exists(
            os.path.join(results_path, results_filename)
        ) and os.path.isfile(os.path.join(results_path, results_filename)):
            print("")
            print("Writing ingest results to S3 bucket...")
            s3_response = (
                self.env["s3_resource"]
                .Object(
                    self.env["aws_dest_bucket"],
                    target,
                )
                .put(Body=open(results_filename, "rb"))
            )
            print(s3_response)
            status = s3_response["ResponseMetadata"]["HTTPStatusCode"]
            if status == 200:
                print("")
                print(
                    f"Results file {os.path.join(self.env['app_img_root_path'],target)} has been uploaded to S3."
                )
        os.chdir(working_dir)

    def create(self, table, attr_dict, item_type, index):
        #create or update the items in the table based on update_metadata flag
        # If update_metadata is enabled, update the item that exists, otherwise create a new item if it doesn't exist
        identifier = attr_dict["identifier"] 
        try:
            # Query the table for existing items with the given identifier
            items = self.query_by_index(table, "Identifier", identifier)
            # Handle case where items already exist and update_metadata is disabled
            if items and len(items) >= 1 and self.env["update_metadata"] == False:
                print(f"Error: Identifier ({identifier}) already exists in {table}. Please update the metadata flag")
                self.log_result(attr_dict,index,2,True)
                return DUPLICATED
            # Handle case where no items exist and update_metadata is disabled
            if not items and self.env["update_metadata"] == False:
                print(f"UPDATE_METADATA is not enabled. Directly creating the item for {identifier}.")
                return self.create_item_in_table(table, attr_dict, item_type, index)  # Directly create the item if update_metadata is disabled
            # Handle case where update_metadata is enabled
            if self.env["update_metadata"]:
                self.update(table, attr_dict, item_type, identifier,index)
        except Exception as e:
            print(f"Error scanning table for identifier '{identifier}': {str(e)}")


    def update(self, table, attr_dict, item_type, identifier, index):
        try:
            # Query the table for existing items with the given identifier
            items = self.query_by_index(table, "Identifier", identifier)
            # Handle case where items already exist and update_metadata is enabled
            if items:
                print(f"Identifier ({identifier}) already exists in {table}.")
                print(f"UPDATE_METADATA is enabled. Updating the existing record for {identifier}.")
                # Retrieve the 'id' of the existing item since it is the partition key
                existing_item_id = items["id"]  # Assuming 'id' is the partition key
                print(f"Existing item ID: {existing_item_id}")
                # Pass the partition key 'id' as an argument to update_items_in_table
                success = self.update_items_in_table(table, existing_item_id, attr_dict, index, identifier)
                if success:
                    print(f"Update for {identifier} was successful.")
                else:
                    print(f"Update for {identifier} failed.")
                return DUPLICATED
            else:
                print(f"Identifier ({identifier}) does not exist in {table}.")
                return self.create_item_in_table(table, attr_dict, item_type, index)  # Create a new item if it doesn't exist
        except Exception as e:
            print(f"Error updating table for identifier '{identifier}': {str(e)}")

    def update_items_in_table(self, table, item_id, attr_dict, index, identifier):
        """
        Updates an existing item in the DynamoDB table with the provided attributes.
        Removes attributes that are empty or None.
        Uses protected keys to ensure certain attributes are not removed.
        Uses the 'id' as the partition key.
        Returns True if the update was successful, False otherwise.
        """
        try:
        # Only allow valid DynamoDB attribute names for both update and remove
            valid_key = lambda k: k and re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", k)

            # Filter out keys that are not in the CSV file and exclude keys containing 'collection' and 'identifier'
            update_keys = {
                key: value
                for key, value in attr_dict.items()
                if key != "identifier"
                   and "collection" not in key.lower()
                   and valid_key(key)
                   and value not in [None, ""]
            }
            # Find keys to remove (those with empty string or None), but protect certain keys
            protected_keys = {"identifier", "createdAt", "updatedAt", "createdat", "updatedat"}
            remove_keys = [
                key for key, value in attr_dict.items()
                if key not in protected_keys
                and "collection" not in key.lower()
                and value in [None, ""]
                and valid_key(key)
            ]
            # If there are no keys to update or remove, return False
            if not update_keys and not remove_keys:
                print(f"No attributes to update or remove for Identifier ({identifier}).")
                return False  # no attributes to update or remove

            # Add updatedAt to the update keys and set it to the current time in utc format
            update_keys["updatedAt"] = self.utcformat(datetime.now())  
            #Initialize the update expression and attribute values
            # 'update_expression_names' maps placeholders (e.g., #key) to actual attribute names. This is used to avoid conflicts with DynamoDB reserved keywords
            update_expression_names = {}
            # 'expression_attribute_values' maps placeholders (e.g., :value) to actual attribute values
            expression_attribute_values = {}
            # 'update_expression_string' will include all attributes to update
            update_expression_string = ""
            remove_expression_string = ""
            #Iterate over the update keys and build the update expression
            for key, value in update_keys.items():
                # Append each key-value pair to the update expression string
                # Use placeholders for attribute names (#key) and values (:value)
                update_expression_string += f"#{key} = :{key},"
                # Add the attribute name to the expression names dictionary to ensure that reserved keywords are handled correctly
                update_expression_names[f"#{key}"] = key
                # Add the attribute value to the expression values dictionary
                expression_attribute_values[f":{key}"] = value 
            # Remove the trailing comma from the update expression string

            update_expression = ""
            if update_expression_string:
                update_expression = "SET " + update_expression_string.rstrip(",")

            # Build the remove expression for valid keys
            for key in remove_keys:
                remove_expression_string += f"#{key},"
                update_expression_names[f"#{key}"] = key
            # Remove the trailing comma from the remove expression string
            if remove_expression_string:
                if update_expression:
                    update_expression += " REMOVE " + remove_expression_string.rstrip(",") # If there are updates, append the remove expression
                else:
                    update_expression = "REMOVE " + remove_expression_string.rstrip(",") # If there are no updates, just remove the attributes

            # Perform the update using the 'id' as the partition key
            # This sends the update request to DynamoDB with the constructed update expression
            table.update_item(
                Key={"id": item_id},  # 'id' is the partition key
                UpdateExpression=update_expression,
                ExpressionAttributeNames=update_expression_names, # Placeholder mappings for attribute names
                ExpressionAttributeValues=expression_attribute_values # Placeholder mappings for attribute values
            )

            self.log_result(
                attr_dict,
                index,
                0,  # Success message index
                True,
            )
            return True  # Indicate success
        except Exception as e:
            print(f"Error updating Identifier ({identifier}) in {table}: {str(e)}")
            self.log_result(
                attr_dict,
                index,
                1,  # Error message index
                False,
            )
            return False  # Indicate failure

    def create_item_in_table(self, table, attr_dict, item_type, index):
        attr_id = str(uuid.uuid4())
        attr_dict["id"] = attr_id
        if item_type == "Collection":
            if "heirarchy_path" in attr_dict:
                attr_dict["heirarchy_path"].append(attr_id)
            else:
                attr_dict["heirarchy_path"] = [attr_id]
        short_id = self.mint_NOID()
        if short_id:
            attr_dict["custom_key"] = os.path.join(
                self.env["noid_scheme"], self.env["noid_naa"], short_id
            )
        utc_now = self.utcformat(datetime.now())
        attr_dict["createdAt"] = utc_now  # Set createdAt to the current time
        attr_dict["updatedAt"] = utc_now  # Set updatedAt to the current time
        success = False
        try:
            if self.env["dry_run"]:
                print(f"PutItem SIMULATED: {attr_dict['identifier']}")
                self.log_result(
                    attr_dict,
                    index,
                    0,
                    True,
                )
            else:
                newRecord = table.put_item(Item=attr_dict)
                success = (newRecord["ResponseMetadata"]["HTTPStatusCode"] == 200)
                if success:
                    print(f"PutItem succeeded: {attr_dict['identifier']}")
                    self.log_result(
                        attr_dict,
                        index,
                        0,
                        True,
                    )
        except Exception as e:
            print(f"Error PutItem failed: {attr_dict['identifier']}")
        if short_id:
            if success:
                long_url = os.path.join(
                    self.env["long_url_path"], item_type.lower(), short_id
                )
                short_url = os.path.join(
                    self.env["short_url_path"],
                    self.env["noid_scheme"],
                    self.env["noid_naa"],
                    short_id,
                )
                self.create_NOID_record(short_id, attr_dict, long_url, short_url, utc_now)
            else:
                self.delete_NOID_record(short_id)

        return attr_id

    def utcformat(self, dt, timespec="milliseconds"):
        # convert datetime to string in UTC format (YYYY-mm-ddTHH:MM:SS.mmmZ)
        iso_str = dt.astimezone(timezone.utc).isoformat("T", timespec)
        return iso_str.replace("+00:00", "Z")

    def process_csv_metadata(self, data_row, item_type):
        attr_dict = {}
        for item in data_row.items():
            # Always set the attribute, even if value is empty
            self.set_attribute(attr_dict, item[0].strip(), str(item[1]).strip().strip("\"").strip())
        # Set embargo flag only after all attributes are processed, based on embargo dates only
        embargo_start = attr_dict.get("embargo_start_date")
        embargo_end = attr_dict.get("embargo_end_date")

        # Add embargo date error checking if start date is after end date:
        if embargo_start and embargo_end:
            try:
                start_dt = parse(str(embargo_start))
                end_dt = parse(str(embargo_end))
                if start_dt > end_dt:
                    print(f"\033[91m⚠️  Error: Embargo start date ({embargo_start}) is after embargo end date ({embargo_end}) for identifier {attr_dict.get('identifier', 'N/A')}\033[0m")
            except Exception as e:
                print(f"Error parsing embargo dates for identifier {attr_dict.get('identifier', 'N/A')}: {e}")

        if (embargo_start and str(embargo_start).strip()) or (embargo_end and str(embargo_end).strip()):
            embargo = True
        else:
            embargo = False
        if ("identifier" not in attr_dict.keys()) or ("title" not in attr_dict.keys()):
            attr_dict = None
            print(f"Missing required attribute in this row!")
        else:
            self.set_attributes_from_env(attr_dict, item_type)
            # Visibility logic: show item only if visibility is True and embargo is False
            visibility = attr_dict.get("visibility", True)
            if visibility and not embargo:
                attr_dict["visibility"] = True
            else:
                attr_dict["visibility"] = False
        return attr_dict

    def set_attributes_from_env(self, attr_dict, item_type):
        if item_type == "Collection":
            attr_dict["collection_category"] = self.env["collection_category"]
        elif item_type == "Archive":
            attr_dict["item_category"] = self.env["collection_category"]
        if "visibility" not in attr_dict.keys():
        
            attr_dict["visibility"] = True

    def set_attribute(self, attr_dict, attr, value):
        lower_attr = attr.lower().replace(" ", "_")
    # Map 'Note' to 'embargo_note'
        if lower_attr == "note":
            lower_attr = "embargo_note"
        if attr == "visibility" or attr == "explicit_content" or attr == "explicit":
            if str(value).strip() == "":
                attr_dict[lower_attr] = True
            elif str(value).lower() == "true":
                attr_dict[lower_attr] = True
            else:
                attr_dict[lower_attr] = False
        elif attr == "embargo_start_date" or attr == "embargo_end_date":
            self.print_index_date(attr_dict, value, lower_attr)
        elif attr == "start_date" or attr == "end_date":
            self.print_index_date(attr_dict, value, lower_attr)
        elif attr == "parent_collection_identifier":
            parent = self.query_by_index(
                self.env["collection_table"], "Identifier", value
            )
            if parent is not None:
                parent_collection_id = parent["id"]
                attr_dict["heirarchy_path"] = parent["heirarchy_path"]
                attr_dict["parent_collection"] = [parent_collection_id]
                attr_dict["parent_collection_identifier"] = [value]
        elif attr == "thumbnail_path":
            attr_dict[lower_attr] = os.path.join(
                self.env["app_img_root_path"],
                self.env["collection_category"],
                value,
                "representative.jpg",
            )
        elif attr == "filename":
            if value.endswith(".pdf") or value.endswith(".jpg"):
                attr_dict["thumbnail_path"] = os.path.join(
                    self.env["app_img_root_path"],
                    self.env["collection_category"],
                    "thumbnail",
                    value.replace(".pdf", ".jpg"),
                )
                attr_dict["manifest_url"] = os.path.join(
                    self.env["app_img_root_path"],
                    self.env["collection_category"],
                    "pdf",
                    value,
                )
            elif "video.vt.edu/media" in value:
                thumbnail = value.split("_")[1]
                attr_dict["thumbnail_path"] = os.path.join(
                    self.env["collection_category"],
                    "thumbnail",
                    thumbnail,
                    ".png",
                )
                attr_dict["manifest_url"] = value
        else:
            extracted_value = self.extract_attribute(attr, value)
            # Always set the attribute, even if extracted_value is falsy (empty string, empty list, etc.)
            attr_dict[lower_attr] = extracted_value

    def print_index_date(self, attr_dict, value, attr):
        # If the value is None, 'None', or an empty string, set the attribute to an empty string.
        # This is to ensure that the attribute is removed from the table if it is not set.
        if value is None or str(value).strip().lower() == "none" or not str(value).strip():
            attr_dict[attr] = ""
            return
        # Check if the value is a year-only format (e.g., "2023")
        if re.fullmatch(r"\d{4}", value):
            attr_dict[attr] = value
            print(f"Year-only detected, attr_dict[{attr}]: {attr_dict[attr]}")
            return
        try:
            parsed_date = parse(value)
            # dates in Elasticsearch are formatted, e.g. "2015/01/01" or
            # "2015/01/01 12:10:30"
            attr_dict[attr] = parsed_date.strftime("%Y/%m/%d")
        except ValueError:
            print(f"Error - Unknown date format: {value} for {attr}")
        except OverflowError:
            print(f"Error - Invalid date range: {value} for {attr}")
        except BaseException as e:
            print(f"Error - Unexpected error: {value} for {attr}, Exception: {e}")

    def query_by_index(self, table, index_name, value):
        index_key = index_name.lower()
        ret_val = None
        try:
            if str(index_key) == "id":
                response = table.query(
                    KeyConditionExpression=Key(index_key).eq(value), Limit=1
                )
            else:
                response = table.query(
                    IndexName=index_name,
                    KeyConditionExpression=Key(index_key).eq(value),
                    Limit=1,
                )
            if "Items" in response and len(response["Items"]) == 1:
                ret_val = response["Items"][0]
        except Exception as e:
            pass
        return ret_val

    def get_collection(self, archive_dict):
        collection = None
        if "collection" in archive_dict:
            collection = self.collection_by_header("collection", archive_dict)
        if not collection:
            if "parent_collection" in archive_dict:
                collection = self.collection_by_header(
                    "parent_collection", archive_dict
                )
        if not collection:
            if "heirarchy_path" in archive_dict:
                collection = self.collection_by_header("heirarchy_path", archive_dict)
        if not collection:
            collection = self.query_by_index(
                self.env["collection_table"],
                "Identifier",
                self.env["collection_identifier"],
            )
        return collection

    def collection_by_header(self, header, archive):
        value = (
            archive[header] if isinstance(archive[header], str) else archive[header][0]
        )
        collection = self.query_by_index(self.env["collection_table"], "id", value)
        if not collection:
            collection = self.query_by_index(
                self.env["collection_table"], "Identifier", value
            )
        return collection

    def extract_attribute(self, header, value):
        if header in self.single_value_headers:
            return value
        elif header in self.multi_value_headers:
            # If the value is empty or only whitespace, return an empty string (so it can be picked up for removal using the update_items_in_table method)        
            if not value.strip():
                return ""  # or return None
            values = value.split("||")
            for idx, val in enumerate(values):
                values[idx] = val.strip()
            return values

    def create_sub_collections(self, parent_collections):
        parent_id = None
        heirarchy_list = []
        for idx in range(len(parent_collections)):
            if idx == 0:
                identifier = parent_collections[idx]
            else:
                identifier += "_" + parent_collections[idx]

            collection_dict = {}
            collection_dict["title"] = parent_collections[idx]
            collection_dict["identifier"] = identifier
            self.set_attributes_from_env(collection_dict, "Collection")
            collection_dict["visibility"] = True
            if parent_id is not None:
                collection_dict["parent_collection"] = [parent_id]
                collection_dict["heirarchy_path"] = heirarchy_list

            items = self.query_by_index(
                self.env["collection_table"], "Identifier", identifier
            )
            if len(items) > 1:
                print(
                    f"Error: Duplicated Identifier ({identifier}) found in {self.env['collection_table']}."
                )
                break
            elif len(items) == 1:
                print(f"Collection {identifier} exists!")
                parent_id = items[0]["id"]
                heirarchy_list = items[0]["heirarchy_path"]
            else:
                parent_id = self.create(
                    self.env["collection_table"], collection_dict, "Collection", idx
                )
                print(f"Collection PutItem succeeded: {identifier}")

        if len(heirarchy_list) > 0:
            self.update_collection_map(heirarchy_list[0])
        return [parent_id]

    def walk_collection(self, parent):
        custom_key = parent["custom_key"].replace("ark:/53696/", "")
        map_location = {
            "id": parent["id"],
            "name": parent["title"],
            "custom_key": custom_key,
        }
        children = self.get_collection_children(parent["id"])
        if len(children) > 0:
            map_location["children"] = []
            for child in children:
                map_location["children"].append(self.walk_collection(child))
        return map_location

    def get_collection_children(self, parent_id):
        scan_kwargs = {
            "FilterExpression": Attr("parent_collection").contains(parent_id),
            "ProjectionExpression": "#id, title, custom_key",
            "ExpressionAttributeNames": {"#id": "id"},
        }
        source_table_items = []
        try:
            done = False
            start_key = None
            while not done:
                if start_key:
                    scan_kwargs["ExclusiveStartKey"] = start_key
                response = self.env["collection_table"].scan(**scan_kwargs)
                source_table_items.extend(response["Items"])
                start_key = response.get("LastEvaluatedKey", None)
                done = start_key is None
        except Exception as e:
            print(f"An error occurred: {str(e)}")
            raise e
        return source_table_items

    def update_collection_map(self, top_parent_id):
        parent = self.query_by_index(self.env["collection_table"], "id", top_parent_id)
        if parent is not None and "parent_collection" not in parent:
            map_obj = self.walk_collection(parent)
            utc_now = self.utcformat(datetime.now())
            if "collectionmap_id" in parent:
                self.env["collectionmap_table"].update_item(
                    Key={"id": parent["collectionmap_id"]},
                    AttributeUpdates={
                        "map_object": {"Value": json.dumps(map_obj), "Action": "PUT"},
                        "collectionmap_category": {
                            "Value": parent["collection_category"],
                            "Action": "PUT",
                        },
                        "updatedAt": {"Value": utc_now, "Action": "PUT"},
                    },
                )
            else:
                map_id = str(uuid.uuid4())
                self.env["collectionmap_table"].put_item(
                    Item={
                        "id": map_id,
                        "map_object": json.dumps(map_obj),
                        "collection_id": parent["id"],
                        "collectionmap_category": parent["collection_category"],
                        "createdAt": utc_now,
                        "updatedAt": utc_now,
                    }
                )
                self.env["collection_table"].update_item(
                    Key={"id": parent["id"]},
                    AttributeUpdates={
                        "collectionmap_id": {"Value": map_id, "Action": "PUT"}
                    },
                )
        else:
            if self.env["dry_run"]:
                print("Collection map creation SIMULATED.")
            else:
                print(
                    f"Error: parent is None or parent['identifier'] is not a top level collection"
                )


    def mint_NOID(self):
        noid = str(uuid.uuid4()).replace("-", "")[:8]
        in_table = self.query_by_index(self.env["mint_table"], "short_id", noid)
        while in_table:
            noid = str(uuid.uuid4()).replace("-", "")[:8]
            in_table = self.query_by_index(self.env["mint_table"], "short_id", noid)

        return noid


    def create_NOID_record(self, noid, item, long_url, short_url, now):
        if self.env["dry_run"]:
            print("create_NOID_record: New NOID SIMULATED.")
            return "12345678"
        category = None
        if "collection_category" in item:
            category = item["collection_category"]
        elif "item_category" in item:
            category = item["item_category"]
        noid_record = {
            "short_id": noid,
            "type": "Collection" if "collection_category" in item else "Item",
            "collection_category": category,
            "identifier": item["identifier"],
            "long_url": long_url,
            "short_url": short_url,
            "created_at": now,
            "hits": 0
        }
        newNoidResponse = self.env["mint_table"].put_item(Item=noid_record)
        success = (newNoidResponse["ResponseMetadata"]["HTTPStatusCode"] == 200)
        if success:
            return noid
        else:
            return None
        

    def delete_NOID_record(self, noid):
        if self.env["dry_run"]:
            print("delete_NOID: SIMULATED.")
        else:
            self.env["mint_table"].delete_item(Key={"noid": noid})
            print(f"delete_NOID: {noid}")