import sys
import time
import boto3, http, io, json, logging, os, uuid, urllib.request
import pandas as pd
from datetime import datetime, timezone
from dateutil.parser import parse
from boto3.dynamodb.conditions import Key, Attr
from botocore.response import StreamingBody
import re

logger = logging.getLogger(__name__)

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
            self.dyndb = boto3.resource("dynamodb", region_name=self.env["REGION"])
            self.dyndb_client = boto3.client("dynamodb")
            self.env["archive_table"] = self.dyndb.Table(self.get_table_name("Archive"))
            self.env["collection_table"] = self.dyndb.Table(
                self.get_table_name("Collection")
            )
            self.env["collectionmap_table"] = self.dyndb.Table(
                self.get_table_name("Collectionmap")
            )
            self.env["mint_table"] = self.dyndb.Table(self.env["DYNAMODB_NOID_TABLE"])
        except Exception as e:
            logger.error(f"An error occurred connecting to an AWS Dynamo resource: {str(e)}")
            raise e

        try:
            self.env["s3_resource"] = boto3.resource("s3")
            self.env["s3_client"] = boto3.client("s3")
        except Exception as e:
            logger.error(f"An error occurred connecting to an AWS s3 resource: {str(e)}")
            raise e

        try:
            headers_file = os.path.join(self.env['APPLICATION_ROOT'],'data','headers_keys.json')
            with open(headers_file) as f:
                headers_keys = json.load(f)
                self.single_value_headers = headers_keys["single_value_headers"]
                self.multi_value_headers = headers_keys["multi_value_headers"]
        except Exception as e:
            logger.error(f"An error occurred reading headers_keys.json: {str(e)}")
            raise e


    def ingest(self):
        metadata_stream = self.get_metadata(self.filename)

        if "INGEST_TYPE" in self.env and self.env['INGEST_TYPE'] == "collection":
            self.batch_import_collections(metadata_stream)
        elif "INGEST_TYPE" in self.env and self.env['INGEST_TYPE'] == "archive":
            self.batch_import_archives(metadata_stream)

        return {"statusCode": 200, "body": json.dumps("Finish metadata import.")}
    
 


    def batch_import_collections(self, response):
        df = self.csv_to_dataframe(io.BytesIO(response["Body"].read()))
        for idx, row in df.iterrows():
            collection_dict = self.process_csv_metadata(row, "Collection")
            if not collection_dict:
                logger.error(f"Error: Collection {idx+1} has failed to be imported.")
                return False
            identifier = collection_dict["identifier"]

            if "thumbnail_path" not in collection_dict or not collection_dict["thumbnail_path"]:
                collection_dict["thumbnail_path"] = os.path.join(
                    self.env["APP_IMG_ROOT_PATH"],
                    self.env["COLLECTION_CATEGORY"],
                    identifier,
                    "representative.jpg",
                )
                
            if "id" not in collection_dict:
                collection_dict["id"] = str(uuid.uuid4())
            
            if "heirarchy_path" not in collection_dict:
                collection_dict["heirarchy_path"] = self.create_heirarchy_path(collection_dict)
                if len(collection_dict['heirarchy_path']) > 1:
                    collection_dict["parent_collection"] = [collection_dict['heirarchy_path'][-2]] 
            
            self.create_item_in_table(self.env["collection_table"], collection_dict, "Collection")

            if "heirarchy_path" in collection_dict:
                self.update_collection_map(collection_dict["heirarchy_path"][0])


    def create_heirarchy_path(self, collection_dict):
        heirarchy_path = []
        parent = None
        parent_identifier = None
        if "parent_collection_identifier" in collection_dict:
            parent_identifier = collection_dict["parent_collection_identifier"]
        elif "PARENT_COLLECTION_IDENTIFIER" in self.env and self.env["PARENT_COLLECTION_IDENTIFIER"] != "":
            parent_identifier = self.env["PARENT_COLLECTION_IDENTIFIER"]
        
        if parent_identifier:
            parent = self.query_by_index(self.env["collection_table"], "Identifier", parent_identifier)

        if parent and "heirarchy_path" in parent:
            heirarchy_path = parent["heirarchy_path"] 
            
        heirarchy_path.append(collection_dict["id"])    
            
        return heirarchy_path


    def batch_import_archives(self, response):
        df = self.csv_to_dataframe(io.BytesIO(response["Body"].read()))
        for idx, row in df.iterrows():
            archive_dict = self.process_csv_metadata(row, "Archive")
            if not archive_dict:
                continue
            else:
                logger.debug(f"archive_dict: {archive_dict}")
                collection = self.get_collection(archive_dict)
                if collection:
                    logger.debug(f"collection: {collection}")
                    archive_dict["collection"] = collection["id"]
                    archive_dict["parent_collection"] = [collection["id"]]
                    archive_dict["heirarchy_path"] = collection["heirarchy_path"]
                    archive_dict["manifest_url"] = os.path.join(
                        self.env["APP_IMG_ROOT_PATH"],
                        self.env["COLLECTION_CATEGORY"],
                        collection["identifier"],
                        archive_dict["identifier"],
                        "manifest.json",
                    )
                    archive_dict["thumbnail_path"] = self.get_thumbnail_path_for_archive(archive_dict, collection)
                    
                    existing_archive = self.query_by_index(self.env["archive_table"], "Identifier", archive_dict["identifier"])
                    if existing_archive:
                        logger.info(f"archive {archive_dict['identifier']} exists in {self.env['archive_table']}")
                        if self.env["UPDATE_METADATA"]:
                            logger.info(f"...updating")
                            self.update_item_in_table(self.env["archive_table"], existing_archive['id'], archive_dict, archive_dict["identifier"])
                        else:
                            logger.info(f"...skipping")
                            continue
                    else:
                        logger.info(f"attempting to create new item {archive_dict['identifier']}")
                        self.create_item_in_table(self.env["archive_table"], archive_dict, "Archive")
                

    
    def archive_exists(self, table, identifier):
        items = self.query_by_index(table, "Identifier", identifier)
        return items and len(items) >= 1

    def get_metadata(self, filename):
        body_encoded = open(filename).read().encode()
        stream = StreamingBody(io.BytesIO(body_encoded), len(body_encoded))
        return {"Body": stream}
    

    def csv_to_dataframe(self, csv_path):
        df = pd.read_csv(
            csv_path,
            na_values="NaN",
            keep_default_na=False,
            encoding="utf-8",
            dtype={"Start Date": str, "End Date": str},
        )
        return df
    

    def get_thumbnail_path_for_archive(self, archive_dict, collection):
        match self.env["MEDIA_TYPE"]:
            case "iiif": 
                return self.get_thumbnail_path_for_iiif(self, archive_dict, collection)
            case _:
                return os.path.join(
                    self.env["APP_IMG_ROOT_PATH"],
                    self.env["COLLECTION_CATEGORY"],
                    collection["identifier"],
                    archive_dict["identifier"],
                    f"{archive_dict['identifier']}_thumbnail.jpg",
                )


    def get_thumbnail_path_for_iiif(self, archive_dict):
        try:
            json_url = urllib.request.urlopen(archive_dict["manifest_url"])
            return json.loads(json_url.read())["thumbnail"]["@id"]
        except Exception as e:
            print(f"Error fetching thumbnail for IIIF archive {archive_dict['identifier']}: {str(e)}")
            return None


    def get_table_name(self, table_name):
        return f"{table_name}-{self.env['DYNAMODB_TABLE_SUFFIX']}" 


    def update_item_in_table(self, table, item_id, attr_dict, identifier):
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
            return True  # Indicate success
        except Exception as e:
            print(f"Error updating Identifier ({identifier}) in {table}: {str(e)}")
            return False  # Indicate failure

    def create_item_in_table(self, table, attr_dict, item_type):
        if "id" not in attr_dict:
            attr_dict["id"] = str(uuid.uuid4())

        short_id = self.mint_NOID()
        if short_id:
            attr_dict["custom_key"] = os.path.join(
                self.env["NOID_SCHEME"], str(self.env["NOID_NAA"]), short_id
            )
        utc_now = self.utcformat(datetime.now())
        attr_dict["createdAt"] = utc_now  # Set createdAt to the current time
        attr_dict["updatedAt"] = utc_now  # Set updatedAt to the current time
        success = False
        try:
            if self.env["DRY_RUN"]:
                print(f"PutItem SIMULATED: {attr_dict['identifier']}")
            else:
                newRecord = table.put_item(Item=attr_dict)
                success = (newRecord["ResponseMetadata"]["HTTPStatusCode"] == 200)
                if success:
                    print(f"PutItem succeeded: {attr_dict['identifier']}")
        except Exception as e:
            print(e)
            print(f"Error PutItem failed: {attr_dict['identifier']}")
        if short_id:
            if success:
                long_url = os.path.join(
                    self.env["LONG_URL_PATH"], item_type.lower(), short_id
                )
                short_url = os.path.join(
                    self.env["SHORT_URL_PATH"],
                    self.env["NOID_SCHEME"],
                    self.env["NOID_NAA"],
                    short_id,
                )
                self.create_NOID_record(short_id, attr_dict, long_url, short_url, utc_now)
            else:
                self.delete_NOID_record(short_id)
        return attr_dict["id"]


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
            
        return attr_dict


    def set_attributes_from_env(self, attr_dict, item_type):
        if item_type == "Collection":
            attr_dict["collection_category"] = self.env["COLLECTION_CATEGORY"]
            if "parent_collection_identifier" not in attr_dict.keys() and len(self.env["PARENT_COLLECTION_IDENTIFIER"]) > 0:
                attr_dict["parent_collection_identifier"] = [self.env["PARENT_COLLECTION_IDENTIFIER"]]
        
        elif item_type == "Archive":
            attr_dict["item_category"] = self.env["COLLECTION_CATEGORY"]
        
        if "visibility" not in attr_dict.keys():
            attr_dict["visibility"] = True

        self.handle_options(attr_dict)



    def handle_options(self, attr_dict):
        return
        


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
            parent = self.query_by_index(self.env["collection_table"], "Identifier", value)

            if parent is not None:
                parent_collection_id = parent["id"]
                attr_dict["parent_collection"] = [parent_collection_id]
                attr_dict["parent_collection_identifier"] = [value]
        elif attr == "thumbnail_path":
            attr_dict[lower_attr] = os.path.join(
                self.env["APP_IMG_ROOT_PATH"],
                self.env["COLLECTION_CATEGORY"],
                value,
                "representative.jpg",
            )
        elif attr == "filename":
            if value.endswith(".pdf") or value.endswith(".jpg"):
                attr_dict["thumbnail_path"] = os.path.join(
                    self.env["APP_IMG_ROOT_PATH"],
                    self.env["COLLECTION_CATEGORY"],
                    "thumbnail",
                    value.replace(".pdf", ".jpg"),
                )
                attr_dict["manifest_url"] = os.path.join(
                    self.env["APP_IMG_ROOT_PATH"],
                    self.env["COLLECTION_CATEGORY"],
                    "pdf",
                    value,
                )
            elif "video.vt.edu/media" in value:
                thumbnail = value.split("_")[1]
                attr_dict["thumbnail_path"] = os.path.join(
                    self.env["APP_IMG_ROOT_PATH"],
                    self.env["COLLECTION_CATEGORY"],
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
            return
        try:
            parsed_date = parse(value)
            # dates in Elasticsearch are formatted, e.g. "2015/01/01" or
            # "2015/01/01 12:10:30"
            # full list of accepted formats:
            # "yyyy/MM/dd HH:mm:ss||yyyy/MM/dd||yyyy/MM||yyyy/M||yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||yyyy-MM||yyyy-M||yyyyMM||yyyy||epoch_millis"
            attr_dict[attr] = parsed_date.strftime("%Y/%m/%d")
        except ValueError:
            print(f"Error - Unknown date format: {value} for {attr}")
        except OverflowError:
            print(f"Error - Invalid date range: {value} for {attr}")
        except BaseException as e:
            print(f"Error - Unexpected error: {value} for {attr}, Exception: {e}")


    def query_by_index(self, table, index_name, value):

        if not table or not index_name or not value:
            print(f"Error: arg is None for query_by_index()")
            print(f"table: {table or 'None'}, index_name: {index_name or 'None'}, value: {value or 'None'}")
            return None
        index_key = index_name.lower()
        response = None
        ret_val = None

        if type(value) is list:
            value = value[0]

        try:
            found = []
            indexes = table.global_secondary_indexes
            for index in indexes:
                found.append(index["IndexName"])
            if index_name in found:
                response = table.query(
                    IndexName=index_name,
                    KeyConditionExpression=Key(index_key).eq(value),
                    Limit=1,
                )
            else:
                response = table.query(
                    KeyConditionExpression=Key(index_key).eq(value),
                    Limit=1,
                )
            if "Items" in response and len(response["Items"]) == 1:
                ret_val = response["Items"][0]
        except Exception as e:
            print(f"Error querying {table} by {index_name}: {str(e)}")
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
                self.env["COLLECTION_IDENTIFIER"],
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
            if self.env["DRY_RUN"]:
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
        if self.env["DRY_RUN"]:
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
        if self.env["DRY_RUN"]:
            print("delete_NOID: SIMULATED.")
        else:
            self.env["mint_table"].delete_item(Key={"short_id": noid})
            print(f"delete_NOID: {noid}")