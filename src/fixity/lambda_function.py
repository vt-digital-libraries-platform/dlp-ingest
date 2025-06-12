import boto3, json, logging, os, uuid
import pandas as pd
from boto3.dynamodb.conditions import Attr
from datetime import datetime
from io import StringIO

"""
    Lambda invocation event must contain: 
        COLLECTION_IDENTIFIER <string>

    All other values will be configured based on lambda environment vars.
    These can be overridden by the invocation event if provided.
    
    FIXITY_TABLE_NAME <string> - DynamoDB table name to write file records to
    S3_BUCKET_NAME <string> - S3 bucket to write results to
    S3_PREFIX <string> - S3 prefix for the collection

    Script expects the csv headers defined in vtdlp/checksumgenerator
    https://github.com/vt-digital-libraries-platform/checksumgenerator
    ['Filename', 'FilePath', 'SHA1_Hash', 'MD5_Hash', 'FileSize', 'FileExtension', 'CreatedDate']
"""

try:
    s3_client = boto3.client("s3")
    dynamo_resource = boto3.resource("dynamodb", region_name="us-east-1")
except Exception as e:
    logging.error(f"Error instantiating aws services. Quitting: {e}")
    raise e


csv_headers = {
    "created": 'CreatedDate',
    "fileExt": 'FileExtension', 
    "fileName": 'Filename', 
    "filePath": 'FilePath', 
    "fileSize": 'FileSize',
    "md5": 'MD5_Hash', 
    "sha1": 'SHA1_Hash'
}



def get_matching_s3_keys(bucket, prefix="", suffix=""):
    s3 = boto3.client("s3")
    kwargs = {"Bucket": bucket, "Prefix": prefix}
    matches = []
    while True:
        resp = s3.list_objects_v2(**kwargs)
        try:
            contents = resp["Contents"]
        except KeyError:
            return
        for obj in contents:
            key = obj["Key"]
            if key.startswith(prefix) and key.endswith(suffix):
                if key not in matches and "/ingest_results/" not in key:
                    matches.append(key)
        try:
            kwargs["ContinuationToken"] = resp["NextContinuationToken"]
        except KeyError:
            break
    return matches


def csv_to_dataframe(csv_path):
    df = pd.read_csv(
        csv_path,
        na_values='NaN',
        keep_default_na=False,
        encoding='utf-8',
        dtype={
            'start_date': str,
            'end_date': str})
    return df

def get_checksum_file_paths(s3_bucket, collection_path):
    checksum_files = get_matching_s3_keys(s3_bucket, os.path.join(collection_path,"checksum"), suffix=".csv")
    return checksum_files


def get_fileList_df(s3_bucket, checksum_file_path):
    dataframe = None
    try:
        response = s3_client.get_object(Bucket=s3_bucket, Key=checksum_file_path)
        data = response["Body"].read().decode('utf-8')
        dataframe = pd.read_csv(StringIO(data))
    except Exception as e:
        logging.error(f"Error fetching/reading file list from {checksum_file_path}. Quitting")
        raise e

    return dataframe


def record_exists_in_db(fixity_table_name, key):
    fixity_table = dynamo_resource.Table(fixity_table_name)

    scan_kwargs = {
            "FilterExpression": Attr("s3_file_path").contains(key),
            "ProjectionExpression": "#id",
            "ExpressionAttributeNames": {"#id": "id"},
        }
    source_table_items = []
    try:
        done = False
        start_key = None
        while not done:
            if start_key:
                scan_kwargs["ExclusiveStartKey"] = start_key
            response = fixity_table.scan(**scan_kwargs)
            source_table_items.extend(response["Items"])
            start_key = response.get("LastEvaluatedKey", None)
            done = start_key is None
    except Exception as e:
        logging.error(f"An error occurred scanning for {key}: {str(e)}")

    return len(source_table_items) > 0


def create_s3_file_metadata(filePath, response):
    metadata = None
    try:
        metadata = response['ResponseMetadata']['HTTPHeaders']
    except KeyError:
        logging.error(f"Error fetching head object for file: {filePath}: {e}")

    return metadata


def write_results_to_s3(s3_bucket, s3_results_path, ingest_job, type, results):
    results_string = "original_file_path,s3_key"
    all_results = ""
    for result in results:
        file_result = ",".join(result)
        all_results += f"\n{file_result}"
    results_string += all_results
    key = os.path.join(s3_results_path, f"{ingest_job}_{type}.csv")
    s3_client.put_object(Bucket=s3_bucket, Key=key, Body=results_string)

def write_summary_to_s3(s3_bucket, s3_results_path, ingest_job, total_files_listed, ingested, existing, not_found):
    results_string = f"Ingest job: {ingest_job}\n\n"
    results_string += f"Files listed in checksum file(s): {total_files_listed}\n"
    results_string += f"Files ingested: {len(ingested)}\n"
    results_string += f"Files previously ingested: {len(existing)}\n"
    results_string += f"Files not found: {len(not_found)}\n\n"

    if len(ingested) + len(existing) == total_files_listed:
        results_string += "All files in checksum lists located successfully.\n"

    logging.info(results_string)
    key = os.path.join(s3_results_path, f"{ingest_job}_summary.txt")
    s3_client.put_object(Bucket=s3_bucket, Key=key, Body=results_string)

def lambda_handler(event, context):
    existing = []
    ingested = []
    not_found = []

    """
        Lambda invocation event must contain: 
            COLLECTION_IDENTIFIER <string>

        All other values will be configured based on lambda environment vars.
        These can be overridden by the invocation event if provided.

        FIXITY_TABLE_NAME <string> - DynamoDB table name to write file records to
        S3_BUCKET_NAME <string> - S3 bucket to write results to
        S3_PREFIX <string> - S3 prefix for the collection
    """
    collection_identifier = event.get('COLLECTION_IDENTIFIER')
    fixity_table_name = event.get('FIXITY_TABLE_NAME') or os.getenv('FIXITY_TABLE_NAME')
    s3_bucket = event.get('S3_BUCKET_NAME') or os.getenv('S3_BUCKET_NAME')
    s3_prefix = event.get('S3_PREFIX') or os.getenv('S3_PREFIX')
    collection_path = os.path.join(s3_prefix, collection_identifier)
    checksum_file_paths = get_checksum_file_paths(s3_bucket, collection_path)
    ingest_job = f"{collection_identifier}-{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')}"
    s3_results_path = os.path.join(collection_path, "ingest_results", ingest_job)

    fixity_table = dynamo_resource.Table(fixity_table_name)

    total_files_listed = 0
    # Process checksum file(s)
    if checksum_file_paths is not None and len(checksum_file_paths) > 0:
        for path in checksum_file_paths:
            file_list = get_fileList_df(s3_bucket, path)
            total_files_listed += len(file_list)
            # Loop through checksum file and process each file listed
            for idx, record in file_list.iterrows():
                created = record[csv_headers['created']]
                fileExt = record[csv_headers['fileExt']]  
                fileName = record[csv_headers['fileName']]
                filePath = record[csv_headers['filePath']]
                fileSize = record[csv_headers['fileSize']]
                md5 = record[csv_headers['md5']]
                sha1 = record[csv_headers['sha1']]

                # find file(s) based on collection path and filename
                obj_keys = get_matching_s3_keys(s3_bucket, collection_path, fileName)
                key = obj_keys[0] if obj_keys else None
                if key:
                    logging.info(f"File found in s3:{s3_bucket}: {key}")
                else:
                    logging.warning(f"File not found: {fileName}")
                    not_found_tuple = (filePath, "not found")
                    if not_found_tuple not in not_found:
                        not_found.append(not_found_tuple)
                    continue
                    

                # Check if fileCharacterization record is already in dynamo
                if record_exists_in_db(fixity_table_name, key):
                    logging.info(f"{filePath} already exists in table: {fixity_table_name} as: {key}")
                    existing_tuple = (filePath, key)
                    if existing_tuple not in existing:
                        existing.append(existing_tuple)
                    continue

                # File found and needs to be ingested
                metadata = None
                mime_type = None
                try:
                    response = s3_client.head_object(Bucket=s3_bucket, Key=key)
                    mime_type = response['ContentType']
                    metadata = create_s3_file_metadata(filePath, response)
                except Exception as e:
                    logging.error(f"Error fetching head object for file: {filePath}")
                    
                ingested_date = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

                file_record = {
                    'id': str(uuid.uuid4()),
                    'collection_identifier': collection_identifier,
                    'file_name': fileName,
                    'file_size': fileSize,
                    'file_extension': fileExt,
                    'file_type': mime_type,
                    'orig_file_path': filePath,
                    's3_bucket': s3_bucket,
                    's3_file_metadata': metadata,
                    's3_file_path': key,
                    'sha1': sha1,
                    'md5': md5,
                    'created_date': created,
                    'file_ingested_date': ingested_date,
                    'ingest_job': ingest_job,
                    'md5_matches_etag': md5 == metadata['etag'].replace('\"', '')
                }
                    
                try:
                    fixity_table.put_item(Item=file_record)
                    ingested_tuple = (filePath, key)
                    if ingested_tuple not in ingested:  
                        ingested.append(ingested_tuple)
                    logging.info(f"File record written to DynamoDB: {filePath}")
                except Exception as e:
                    logging.error(f"Error writing to DynamoDB for file: {filePath}: {e}")

# ================================================

    # Completed. Log summary info
    logging.info(f"Completed. Writing results to {s3_bucket}:{s3_results_path}")
    logging.info(f"Total files listed in checksum file(s): {total_files_listed}")

    if len(existing) > 0:
        logging.info(f"{len(existing)} files were previously ingested and are recorded in dynamo: {existing}")
        write_results_to_s3(s3_bucket, s3_results_path, ingest_job, "previously_ingested", existing)
    if len(not_found) > 0:
        logging.warning(f"Could not find {len(not_found)} file(s): {not_found}")
        write_results_to_s3(s3_bucket, s3_results_path, ingest_job, "not_found", not_found)
    if len(ingested) > 0:
        logging.info(f"Found {len(ingested)} file(s) in S3: {ingested}")
        write_results_to_s3(s3_bucket, s3_results_path, ingest_job, "ingested", ingested)
    
    write_summary_to_s3(s3_bucket, s3_results_path, ingest_job, total_files_listed, ingested, existing, not_found)
    
    logging.info("Process completed")
    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Process completed.",
        }),
    }
