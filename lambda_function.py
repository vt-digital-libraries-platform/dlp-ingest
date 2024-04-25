import csv
import http
import json
import urllib.parse
import urllib.request
import requests
import boto3
import io
import sys
import pandas as pd
from datetime import datetime, timezone
from dateutil.parser import parse
import uuid
import os
from boto3.dynamodb.conditions import Key, Attr
from botocore.response import StreamingBody

# Global variables

results = []

# Environment variables
env = {}
env["aws_dest_bucket"] = os.getenv("AWS_DEST_BUCKET")
env["collection_category"] = os.getenv("COLLECTION_CATEGORY")
env["collection_identifier"] = os.getenv("COLLECTION_IDENTIFIER")
env["region_name"] = os.getenv("REGION")
env["dynamodb_table_suffix"] = os.getenv("DYNAMODB_TABLE_SUFFIX")
env["app_img_root_path"] = os.getenv("APP_IMG_ROOT_PATH")
env["noid_scheme"] = os.getenv("NOID_SCHEME")
env["noid_naa"] = os.getenv("NOID_NAA")
env["long_url_path"] = os.getenv("LONG_URL_PATH")
env["short_url_path"] = os.getenv("SHORT_URL_PATH")
env["api_key"] = os.getenv("API_KEY")
env["api_endpoint"] = os.getenv("API_ENDPOINT")


def get_table_name(table_name):
    return f"{table_name}-{env['dynamodb_table_suffix']}"


try:
    dyndb = boto3.resource("dynamodb", region_name=env["region_name"])
    s3 = boto3.resource("s3")
    archive_table = dyndb.Table(get_table_name("Archive"))
    collection_table = dyndb.Table(get_table_name("Collection"))
    collectionmap_table = dyndb.Table(get_table_name("Collectionmap"))
except Exception as e:
    print(f"An error occurred connecting to an AWS resource: {str(e)}")
    raise e

single_value_headers = None
multi_value_headers = None
DUPLICATED = "Duplicated"
try:
    headers_json = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "lib_files/data/20240425_headers_keys.json",
    )
    with open(headers_json) as f:
        headers_keys = json.load(f)
        single_value_headers = headers_keys["single_value_headers"]
        multi_value_headers = headers_keys["multi_value_headers"]
except Exception as e:
    print(f"An error occurred reading headers_keys.json: {str(e)}")
    raise e


def local_handler(filename):
    body_encoded = open(filename).read().encode()
    stream = StreamingBody(io.BytesIO(body_encoded), len(body_encoded))
    response = {"Body": stream}

    if "collection_metadata.csv" in filename:
        batch_import_collections(response)
    elif "archive_metadata.csv" in filename:
        batch_import_archives(response)
    else:
        print(f"Error: {filename} is not a valid filename.")
        print(
            "Filenames must end with [ _collection_metadata.csv | _archive_metadata.csv ] in order to be processed."
        )
    print("Results: ===================================================")
    print("Total records processed: ", len(results))
    print_results()


def lambda_handler(event, context):
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = urllib.parse.unquote_plus(
        event["Records"][0]["s3"]["object"]["key"], encoding="utf-8"
    )
    try:
        s3 = boto3.client("s3")
        response = s3.get_object(Bucket=bucket, Key=key)
        if "_collection_metadata.csv" in key:
            batch_import_collections(response)
        elif "_archive_metadata.csv" in key:
            batch_import_archives(response)
        else:
            error_message = f"Error: {key} is not a valid filename."
            print(error_message)
            print(
                "Filenames must end with [ _collection_metadata.csv | _archive_metadata.csv ] in order to be processed."
            )
            return {"statusCode": 200, "body": json.dumps(error_message)}
    except Exception as e:
        print(f"An error occurred importing {key} from bucket {bucket}: {str(e)}")
        raise e
    else:
        print("Results: ===================================================")
        print("Total records processed: ", len(results))
        print_results()
        return {"statusCode": 200, "body": json.dumps("Finish metadata import.")}


def csv_to_dataframe(csv_path):
    df = pd.read_csv(
        csv_path,
        na_values="NaN",
        keep_default_na=False,
        encoding="utf-8",
        dtype={"Start Date": str, "End Date": str},
    )
    df = header_update(df)
    return df


def status_message(msg_index, csv_index, attr_dict, succeeded):
    messages = {
        0: f"Row {csv_index}: {attr_dict['identifier']} has been successfully imported. Succeeded: {str(succeeded)}",
        1: f"Error Row {csv_index}: Invalid {attr_dict['identifier']} record defined in row. Succeeded: {str(succeeded)}",
        2: f"Error Row {csv_index}: Identifier ({attr_dict['identifier']}) already exists in Dynamo table. Succeeded: {str(succeeded)}",
        3: f"Error Row {csv_index}: Collection record not found for Archive {attr_dict['identifier']}. Succeeded: {str(succeeded)}",
        4: f"Error Row {csv_index}: manifest.json not found for Archive {attr_dict['identifier']} in s3 bucket. Succeeded: {str(succeeded)}",
    }
    return messages.get(msg_index)


def log_result(attr_dict, csv_index, message_index, succeeded=True):
    status = "succeeded" if succeeded else "failed"
    identifier = (
        attr_dict["identifier"] if (attr_dict and "identifier" in attr_dict) else "N/A"
    )
    if identifier == "N/A":
        status = "failed"
    results.append(
        {
            "row_in_metadata": csv_index + 2,
            "identifier": identifier,
            "succeeded": succeeded,
            "message": status_message(
                message_index, csv_index + 2, attr_dict, succeeded
            ),
        }
    )


def print_results():
    df = pd.DataFrame(results)
    results_filename = f"{env['collection_identifier']}_ingest_results_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
    working_dir = os.path.abspath(os.path.dirname(__file__))
    results_path = os.path.join(working_dir, "results_files")
    if not os.path.exists(results_path):
        os.mkdir(results_path)
    os.chdir(results_path)
    df.to_csv(results_filename, index=False)
    s3_response = None
    target = os.path.join(
        env["collection_category"],
        env["collection_identifier"],
        "metadata_import_results",
        results_filename,
    )
    if os.path.exists(os.path.join(results_path, results_filename)) and os.path.isfile(
        os.path.join(results_path, results_filename)
    ):
        print("")
        print("Writing ingest results to S3 bucket...")
        s3_response = s3.Object(
            env["aws_dest_bucket"],
            target,
        ).put(Body=open(results_filename, "rb"))
        print(s3_response)
        status = s3_response["ResponseMetadata"]["HTTPStatusCode"]
        if status == 200:
            print("")
            print(
                f"Results file {os.path.join(env['app_img_root_path'],target)} has been uploaded to S3."
            )
    os.chdir(working_dir)


def batch_import_collections(response):
    df = csv_to_dataframe(io.BytesIO(response["Body"].read()))
    for idx, row in df.iterrows():
        print("")
        print("===================================")
        collection_dict = process_csv_metadata(row, "Collection")
        if not collection_dict:
            print(f"Error: Collection {idx+1} has failed to be imported.")
            log_result(
                False,
                idx,
                1,
                False,
            )
            break
        identifier = collection_dict["identifier"]
        items = query_by_index(collection_table, "Identifier", identifier)
        if len(items) >= 1:
            print(
                f"Error: Identifier ({identifier}) already exists in {collection_table}."
            )
            # TODO: Update existing collection implementation.
            break
        else:
            collection_dict["thumbnail_path"] = os.path.join(
                env["app_img_root_path"],
                env["collection_category"],
                identifier,
                "representative.jpg",
            )
            create_if_not_exists(collection_table, collection_dict, "Collection", idx)
        if "heirarchy_path" in collection_dict:
            update_collection_map(collection_dict["heirarchy_path"][0])
        print(f"Collection {idx+1} ({identifier}) has been imported.")


def batch_import_archives(response):
    df = csv_to_dataframe(io.BytesIO(response["Body"].read()))
    for idx, row in df.iterrows():
        print("")
        print("===================================")
        archive_dict = process_csv_metadata(row, "Archive")
        if not archive_dict:
            print(f"Error: Archive {idx+1} has failed to be imported.")
            log_result(
                False,
                idx,
                1,
                False,
            )
            break
        else:
            collection = get_collection(archive_dict)
            if not collection:
                print(f"Error: Collection not found for Archive {idx+1}.")
                print("Error: Archive must belong to a collection to be ingested")
                log_result(
                    archive_dict,
                    idx,
                    3,
                    False,
                )
                break
            collection_identifier = (
                collection["identifier"] if collection else env["collection_identifier"]
            )
            if collection_identifier is None:
                print(f"Error: Collection not found for Archive {idx+1}.")
                print("Error: Archive must belong to a collection to be ingested")
                log_result(
                    archive_dict,
                    idx,
                    3,
                    False,
                )
                break
            else:
                archive_dict["identifier"] = (
                    archive_dict["identifier"]
                    if collection_identifier in archive_dict["identifier"]
                    else f"{collection_identifier}_{archive_dict['identifier']}"
                )
                archive_dict["collection"] = collection["id"]
                archive_dict["parent_collection"] = [collection["id"]]
                archive_dict["heirarchy_path"] = collection["heirarchy_path"]
                archive_dict["manifest_url"] = os.path.join(
                    env["app_img_root_path"],
                    env["collection_category"],
                    collection_identifier,
                    archive_dict["identifier"],
                    "manifest.json",
                )
                try:
                    json_url = urllib.request.urlopen(archive_dict["manifest_url"])
                    archive_dict["thumbnail_path"] = json.loads(json_url.read())[
                        "thumbnail"
                    ]["@id"]
                except (urllib.error.HTTPError, http.client.IncompleteRead) as http_err:
                    print(http_err)
                    print(f"{archive_dict['manifest_url']} not found.")
                    log_result(
                        archive_dict,
                        idx,
                        4,
                        False,
                    )
                except Exception as e:
                    print(e)
                    print(f"{archive_dict['manifest_url']} not found.")
                    log_result(
                        archive_dict,
                        idx,
                        4,
                        False,
                    )

                if (
                    "thumbnail_path" in archive_dict
                    and len(archive_dict["thumbnail_path"]) > 0
                ):
                    create_if_not_exists(archive_table, archive_dict, "Archive", idx)


def get_collection(archive_dict):
    collection = None
    if "collection" in archive_dict:
        collection = collection_by_header("collection", archive_dict)
    if not collection:
        if "parent_collection" in archive_dict:
            collection = collection_by_header("parent_collection", archive_dict)
    if not collection:
        if "heirarchy_path" in archive_dict:
            collection = collection_by_header("heirarchy_path", archive_dict)
    if not collection:
        collection = query_by_index(
            collection_table, "Identifier", env["collection_identifier"]
        )
    return collection


def collection_by_header(header, archive):
    value = archive[header] if isinstance(archive[header], str) else archive[header][0]
    collection = query_by_index(collection_table, "id", value)
    if not collection:
        collection = query_by_index(collection_table, "Identifier", value)
    return collection


def header_update(records):
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


def create_if_not_exists(table, attr_dict, item_type, index):
    identifier = attr_dict["identifier"]
    items = query_by_index(table, "Identifier", identifier)
    if items is not None and len(items) >= 1:
        print(f"Identifier ({identifier}) already exists in {table}.")
        # TODO: Update existing item implementation.
        log_result(
            attr_dict,
            index,
            2,
            True,
        )
        return DUPLICATED
    else:
        return create_item_in_table(table, attr_dict, item_type, index)


def create_item_in_table(table, attr_dict, item_type, index):
    attr_id = str(uuid.uuid4())
    attr_dict["id"] = attr_id
    if item_type == "Collection":
        if "heirarchy_path" in attr_dict:
            attr_dict["heirarchy_path"].append(attr_id)
        else:
            attr_dict["heirarchy_path"] = [attr_id]

    short_id = mint_NOID()
    if short_id:
        attr_dict["custom_key"] = env["noid_scheme"] + env["noid_naa"] + "/" + short_id
    now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    utc_now = utcformat(datetime.now())
    attr_dict["createdAt"] = utc_now
    attr_dict["updatedAt"] = utc_now
    try:
        table.put_item(Item=attr_dict)
        print(f"PutItem succeeded: {attr_dict['identifier']}")
        log_result(
            attr_dict,
            index,
            0,
            True,
        )
    except Exception as e:
        print(f"Error PutItem failed: {attr_dict['identifier']}")
    if short_id:
        # after NOID is created and item is inserted, update long_url and
        # short_url through API
        long_url = os.path.join(env["long_url_path"], item_type.lower(), short_id)
        short_url = os.path.join(
            env["short_url_path"], env["noid_scheme"], env["noid_naa"], short_id
        )
        update_NOID(long_url, short_url, short_id, now)
    return attr_id


def utcformat(dt, timespec="milliseconds"):
    # convert datetime to string in UTC format (YYYY-mm-ddTHH:MM:SS.mmmZ)
    iso_str = dt.astimezone(timezone.utc).isoformat("T", timespec)
    return iso_str.replace("+00:00", "Z")


def process_csv_metadata(data_row, item_type):
    attr_dict = {}
    for items in data_row.items():
        if items[0].strip() and str(items[1]).strip():
            set_attribute(attr_dict, items[0].strip(), str(items[1]).strip())
    if ("identifier" not in attr_dict.keys()) or ("title" not in attr_dict.keys()):
        attr_dict = None
        print(f"Missing required attribute in this row!")
    else:
        set_attributes_from_env(attr_dict, item_type)
    return attr_dict


def set_attributes_from_env(attr_dict, item_type):
    if item_type == "Collection":
        attr_dict["collection_category"] = env["collection_category"]
        if "visibility" not in attr_dict.keys():
            attr_dict["visibility"] = False
    elif item_type == "Archive":
        attr_dict["item_category"] = env["collection_category"]
        attr_dict["visibility"] = True


def set_attribute(attr_dict, attr, value):
    lower_attr = attr.lower().replace(" ", "_")
    if attr == "visibility" or attr == "explicit_content" or attr == "explicit":
        if str(value).lower() == "true":
            attr_dict[lower_attr] = True
        else:
            attr_dict[lower_attr] = False
    elif attr == "start_date" or attr == "end_date":
        print_index_date(attr_dict, value, lower_attr)
    elif attr == "parent_collection":
        items = query_by_index(collection_table, "identifier", value)
        if len(items) == 1:
            parent_collection_id = items[0]["id"]
            attr_dict["heirarchy_path"] = items[0]["heirarchy_path"]
            attr_dict[lower_attr] = [parent_collection_id]
    elif attr == "thumbnail_path":
        attr_dict[lower_attr] = os.path.join(
            env["app_img_root_path"],
            env["collection_category"],
            value,
            "representative.jpg",
        )
    elif attr == "filename":
        if value.endswith(".pdf") or value.endswith(".jpg"):
            attr_dict["thumbnail_path"] = os.path.join(
                env["app_img_root_path"],
                env["collection_category"],
                "thumbnail",
                value.replace(".pdf", ".jpg"),
            )
            attr_dict["manifest_url"] = os.path.join(
                env["app_img_root_path"], env["collection_category"], "pdf", value
            )
        elif "video.vt.edu/media" in value:
            thumbnail = value.split("_")[1]
            attr_dict["thumbnail_path"] = os.path.join(
                env["app_img_root_path"],
                env["collection_category"],
                "thumbnail",
                thumbnail,
                ".png",
            )
            attr_dict["manifest_url"] = value
    else:
        extracted_value = extract_attribute(attr, value)
        if extracted_value:
            attr_dict[lower_attr] = extracted_value


def print_index_date(attr_dict, value, attr):
    try:
        parsed_date = parse(value)
        # dates in Elasticsearch are formatted, e.g. "2015/01/01" or
        # "2015/01/01 12:10:30"
        attr_dict[attr] = parsed_date.strftime("%Y/%m/%d")
    except ValueError:
        print(f"Error - Unknown date format: {value} for {attr}")
    except OverflowError:
        print(f"Error - Invalid date range: {value} for {attr}")
    except BaseException:
        print(f"Error - Unexpected error: {value} for {attr}")


def query_by_index(table, index_name, value):
    index_key = index_name.lower()
    ret_val = None
    try:
        response = table.query(
            IndexName=index_name,
            KeyConditionExpression=Key(index_key).eq(value),
            Limit=1,
        )
        if "Items" in response and len(response["Items"]) == 1:
            ret_val = response["Items"][0]
    except Exception as e:
        print(f"An error occurred querying {table.name} by {index_name}: {value}")
        print(str(e))
    return ret_val


def extract_attribute(header, value):
    if header in single_value_headers:
        return value
    elif header in multi_value_headers:
        return value.split("||")


def create_sub_collections(parent_collections):
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
        set_attributes_from_env(collection_dict, "Collection")
        collection_dict["visibility"] = True
        if parent_id is not None:
            collection_dict["parent_collection"] = [parent_id]
            collection_dict["heirarchy_path"] = heirarchy_list

        items = query_by_index(collection_table, "Identifier", identifier)
        if len(items) > 1:
            print(
                f"Error: Duplicated Identifier ({identifier}) found in {collection_table}."
            )
            break
        elif len(items) == 1:
            print(f"Collection {identifier} exists!")
            parent_id = items[0]["id"]
            heirarchy_list = items[0]["heirarchy_path"]
        else:
            parent_id = create_if_not_exists(
                collection_table, collection_dict, "Collection", idx
            )
            print(f"Collection PutItem succeeded: {identifier}")

    if len(heirarchy_list) > 0:
        update_collection_map(heirarchy_list[0])
    return [parent_id]


def update_collection_map(top_parent_id):
    parent = query_by_index(collection_table, "id", top_parent_id)
    if "parent_collection" not in parent:
        map_obj = walk_collection(parent)
        utc_now = utcformat(datetime.now())
        if "collectionmap_id" in parent:
            collectionmap_table.update_item(
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
            collectionmap_table.put_item(
                Item={
                    "id": map_id,
                    "map_object": json.dumps(map_obj),
                    "collection_id": parent["id"],
                    "collectionmap_category": parent["collection_category"],
                    "createdAt": utc_now,
                    "updatedAt": utc_now,
                }
            )

            collection_table.update_item(
                Key={"id": parent["id"]},
                AttributeUpdates={
                    "collectionmap_id": {"Value": map_id, "Action": "PUT"}
                },
            )
    else:
        print(f"Error: {parent['identifier']} is not a top level collection")


def get_collection_children(parent_id):
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
            response = collection_table.scan(**scan_kwargs)
            source_table_items.extend(response["Items"])
            start_key = response.get("LastEvaluatedKey", None)
            done = start_key is None

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        raise e
    return source_table_items


def walk_collection(parent):
    custom_key = parent["custom_key"].replace("ark:/53696/", "")
    map_location = {
        "id": parent["id"],
        "name": parent["title"],
        "custom_key": custom_key,
    }
    children = get_collection_children(parent["id"])
    if len(children) > 0:
        map_location["children"] = []
        for child in children:
            map_location["children"].append(walk_collection(child))

    return map_location


def mint_NOID():
    headers = {"x-api-key": env["api_key"]}
    url = env["api_endpoint"] + "mint"
    response = requests.get(url, headers=headers)
    print(f"mint_NOID {response.text}")
    if response.status_code == 200:
        res_message = (response.json())["message"]
        start_idx = res_message.find("New NOID: ") + len("New NOID: ")
        end_idx = res_message.find(" is created.", start_idx)
        return res_message[start_idx:end_idx]
    else:
        return None


def update_NOID(long_url, short_url, noid, create_date):
    headers = {"x-api-key": env["api_key"]}
    body = (
        "long_url="
        + long_url
        + "&short_url="
        + short_url
        + "&noid="
        + noid
        + "&create_date="
        + create_date
    )
    url = env["api_endpoint"] + "update"
    response = requests.post(url, data=body, headers=headers)
    print(f"update_NOID: {response.text}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 lambda_function.py <filename>")
        sys.exit(1)
    else:
        filename = "".join(sys.argv[1])
        local_handler(filename)
