import json
import boto3
import logging
from decimal import Decimal
import uuid
import sys
import os
import shutil
from decimal import Decimal
from botocore.exceptions import ClientError
from collections import defaultdict
from boto3.dynamodb.conditions import Key
import datetime

# Set up logging to write to a file with a timestamp in the filename
log_filename = f"textract_lambda_output_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    filename=log_filename,
    filemode='a',
    format='%(asctime)s %(levelname)s: %(message)s',
    level=logging.INFO
)
logger = logging.getLogger()
logger.setLevel("INFO")

# Utility: recursively convert all float values to Decimal for DynamoDB
# This is needed because DynamoDB does not accept float types, only Decimal.
def convert_floats(obj):
    # If the object is a float, convert it to Decimal
    if isinstance(obj, float):
        return Decimal(str(obj))
    # If the object is a dictionary, recursively convert its values
    elif isinstance(obj, dict):
        return {k: convert_floats(v) for k, v in obj.items()}
    # If the object is a list, recursively convert its elements
    elif isinstance(obj, list):
        return [convert_floats(i) for i in obj]
    # Otherwise, return the object as is
    else:
        return obj

# AWS clients and logger setup
print("Initializing AWS Textract client and logger...")
logger.info("Initializing AWS Textract client and logger...")
textract_client = boto3.client('textract')  # Textract client for OCR
logger = logging.getLogger()                # Logger for debugging/info
logger.setLevel("INFO")

# Things to DO
# 1. Parse collection name and filename from the message
# 2. Create folders 
# 3. Integrate texteact logic

#Left: change the output path and collectionanme using split

# Parse Textract response and write line information to DynamoDB
def getLineInformation(collection_of_textract_responses, response_filename):
    logger.info("Parsing line information from Textract response...")
    print("Parsing line information from Textract response...")
    # This method ingests the Textract responses in JSON format, parses it and returns the line information
    total_text_with_info = []
    running_sequence_number = 0
    logger.info(f"Response filename: {response_filename}")
    blocks = collection_of_textract_responses['Blocks']
    logger.info(f"Number of blocks in response: {len(blocks)}")

    # DynamoDB setup
    logger.info("Setting up DynamoDB resource and table for line info...")
    print("Setting up DynamoDB resource and table for line info...")
    dynamodb = boto3.resource('dynamodb')
    line_table_name =  os.getenv("TEXTRACT_LINE_TABLE")
    line_table = dynamodb.Table(line_table_name)
    print("Scanning DynamoDB for existing items in textract line table using the unique_key...")
    # Iterate through all blocks in the Textract response
    for block in blocks:
        logger.info(f"Processing block: {block.get('BlockType')}")
        # Only process blocks of type 'LINE'
        if block['BlockType'] == 'LINE':
            item = {}
            running_sequence_number += 1
            logger.info(f"Line block found. Sequence number: {running_sequence_number}")
            print(f"Line block found. Sequence number: {running_sequence_number}")
            # 1. Extract identifier: everything but the last digits before underscore
            # Example: "fchs_1950_001_015_001" -> "fchs_1950_001_015"
            item['id'] =str(uuid.uuid4())  # Use the same UUID
            identifier_full=response_filename
            logger.info(f"Original identifier from JSON: {identifier_full}")
            print(f"Original identifier from JSON: {identifier_full}")
            identifier_parts = identifier_full.split('_')
            if len(identifier_parts) > 1:
                item['identifier'] = '_'.join(identifier_parts[:-1])
            else:
                item['identifier'] = identifier_full
            logger.info(f"Processed identifier (without last digits): {item['identifier']}")
            print(f"Processed identifier (without last digits): {item['identifier']}")

            # 2. Set identifier_page to the whole identifier

            #item['child_ids'] = block.get('child_ids', [])
            relation = block.get('Relationships', [])
            child_ids = []
            # Extract child IDs from relationships if present
            for sub in relation:
                child_ids.extend(sub.get('Ids', []))
            item['child_ids'] = child_ids
            logger.info(f"Child IDs: {item['child_ids']}")
            item['collection_category'] = 'federated'
            #item['collection_id'] = block.get('collectionId', '')
            item['confidence'] = block.get('Confidence', 0)
            logger.info(f"Confidence: {item['confidence']}")
            from datetime import datetime, timezone
            now = datetime.now(timezone.utc).isoformat()
            item['created_at'] = now
            #item['updated_at'] = block.get('updated_at', '')
            # Geometry info
            geom = block.get('Geometry', {})
            logger.info(f"Geometry: {geom}")
            bounding_box = geom.get('BoundingBox', {})
            polygon = geom.get('Polygon', [])
            # Store geometry information as strings for DynamoDB compatibility
            item['geometry'] = {
                'boundingbox': {
                    'Height': str(bounding_box.get('Height', '')),
                    'Left': str(bounding_box.get('Left', '')),
                    'Top': str(bounding_box.get('Top', '')),
                    'Width': str(bounding_box.get('Width', '')),
                },
                'polygon': [
                    {'X': str(p.get('X', '')), 'Y': str(p.get('Y', ''))} for p in polygon
                ]
            }
            item['identifier_page'] = identifier_full
            logger.info(f"identifier_page set to: {item['identifier_page']}")
            item['isactive'] = True #default
            item['output_id'] = block.get('Id','')
            item['line_no'] = running_sequence_number
            item['output_text'] = block.get('Text', '')
            item['visibility'] = True
            # Scan for existing item with same output_id
            item['unique_key'] = f"{item['identifier_page']}_{item['line_no']}"  # or line_no for lines
            logger.info(f"Scanning DynamoDB for existing item with unique_key: {item['unique_key']}")
            print(f"Scanning DynamoDB for existing item with unique_key: {item['unique_key']}")
            response = line_table.query(
                IndexName='unique_key-index',  # Replace with your actual index name
                KeyConditionExpression=Key('unique_key').eq(item['unique_key'])
            )
            if response['Items']:
                # Item exists
                print(f"\033[91m❗ WARNING: Item with unique_key {item['unique_key']} already exists in DynamoDB. Skipping line insert/update.\033[0m", file=sys.stderr)
                logger.warning(f"Item with unique_key {item['unique_key']} already exists in DynamoDB. Skipping line insert/update.")
                print(f"Item with unique_key {item['unique_key']} already exists in DynamoDB. Skipping line insert/update.")
                continue  # Skip updating/inserting this item
            else:
                print("No existing item found. Adding new item to DynamoDB.")
                logger.info("No existing item found. Adding new item to DynamoDB.")
                line_table.put_item(Item=convert_floats(item))
            total_text_with_info.append(item)
    return total_text_with_info

def getWordInformation(collection_of_textract_responses, response_filename):
    logger.info("Parsing word information from Textract response...")
    print("Parsing word information from Textract response...")
    # This method ingests the texteact responses in JSON format, parses it and returns the word information
    total_text_with_info = []
    running_sequence_number = 0
    logger.info(f"Response filename: {response_filename}")
    blocks = collection_of_textract_responses['Blocks']
    logger.info(f"Number of blocks in response: {len(blocks)}")

    # DynamoDB setup
    logger.info("Setting up DynamoDB resource and table for word info...")
    print("Setting up DynamoDB resource and table for word info...")
    dynamodb = boto3.resource('dynamodb')
    word_table_name = os.getenv("TEXTRACT_WORD_TABLE")
    word_table = dynamodb.Table(word_table_name)
    print("Scanning DynamoDB for existing items in textract word table using the unique_key...")
    # Iterate through all blocks in the Textract response
    for block in blocks:
        logger.info(f"Processing block: {block.get('BlockType')}")
        # Only process blocks of type 'WORD'
        if block['BlockType'] == 'WORD':
            item = {}
            running_sequence_number += 1
            logger.info(f"Word block found. Sequence number: {running_sequence_number}")
            item['id'] = str(uuid.uuid4())
            identifier_full = response_filename
            logger.info(f"Original identifier from JSON: {identifier_full}")
            identifier_parts = identifier_full.split('_')
            if len(identifier_parts) > 1:
                item['identifier'] = '_'.join(identifier_parts[:-1])
            else:
                item['identifier'] = identifier_full
            logger.info(f"Processed identifier (without last digits): {item['identifier']}")
            item['collection_category'] = 'federated'
            item['confidence'] = block.get('Confidence', 0)
            logger.info(f"Confidence: {item['confidence']}")
            from datetime import datetime, timezone
            now = datetime.now(timezone.utc).isoformat()
            item['created_at'] = now
            geom = block.get('Geometry', {})
            logger.info(f"Geometry: {geom}")
            bounding_box = geom.get('BoundingBox', {})
            polygon = geom.get('Polygon', [])
            # Store geometry information as strings for DynamoDB compatibility
            item['geometry'] = {
                'boundingbox': {
                    'Height': str(bounding_box.get('Height', '')),
                    'Left': str(bounding_box.get('Left', '')),
                    'Top': str(bounding_box.get('Top', '')),
                    'Width': str(bounding_box.get('Width', '')),
                },
                'polygon': [
                    {'X': str(p.get('X', '')), 'Y': str(p.get('Y', ''))} for p in polygon
                ]
            }
            item['identifier_page'] = identifier_full
            logger.info(f"identifier_page set to: {item['identifier_page']}")
            item['isactive'] = True
            item['output_id'] = block.get('Id','')
            logger.info(f"output_id set to: {item['output_id']}")
            item['texttype'] = block.get('TextType', '')
            item['wordtext'] = block.get('Text', '')
            logger.info(f"Word text: {item['wordtext']}")
            item['word_no'] = running_sequence_number
            item['visibility'] = True

            # Scan for existing item with same output_id
            #logger.info(f"Scanning DynamoDB for existing item with output_id: {item['output_id']}")
            item['unique_key'] = f"{item['identifier_page']}_{item['word_no']}"  # or line_no for lines
            response = word_table.query(
                IndexName='unique_key-index',
                KeyConditionExpression=Key('unique_key').eq(item['unique_key'])
            )
            if response['Items']:
                logger.warning(f"Item with unique_key {item['unique_key']} already exists. Skipping word insert/update.")
                print(f"\033[91m❗ WARNING: Item with unique_key {item['unique_key']} already exists. Skipping word insert/update.\033[0m", file=sys.stderr)
                continue  # Skip updating/inserting this item
            else:
                logger.info(f"No existing item found. Adding new item to DynamoDB. {item}")
                print(f"No existing item found. Adding new item to DynamoDB. {item}")
                word_table.put_item(Item=convert_floats(item))
            total_text_with_info.append(item)
    return total_text_with_info

def parseJSON(jsonObject, response_filename):
    logger.info("Parsing JSON object for line and word information...")
    print("Parsing JSON object for line and word information...")#
    # Helper function to parse line and word information. Takes in AWS JSON response
    parsed_json = json.loads(jsonObject)
    #shared_uuid = str(uuid.uuid4())  # Generate once
    line_info = getLineInformation(parsed_json, response_filename)
    word_info = getWordInformation(parsed_json, response_filename)
    return line_info, word_info

def clean_tmp():
    tmp_dir = '/tmp'
    for filename in os.listdir(tmp_dir):
        file_path = os.path.join(tmp_dir, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print(f'Failed to delete {file_path}. Reason: {e}')
            logger.error(f'Failed to delete {file_path}. Reason: {e}')

def preprocess_image_for_textract(s3, source_bucket, image, output_path, response_filename, textract_bucket):
    """
    Preprocess image using OpenCV, upload to Textract bucket, and return image_document for Textract.
    """
    import uuid
    unique_id = str(uuid.uuid4())
    local_image_path = f'/tmp/preprocess_image_{unique_id}.jpg'
    logger.info(f"Downloading image from S3 to: {local_image_path}")
    print(f"Downloading image from S3 to: {local_image_path}")
    s3.download_file(source_bucket, image, local_image_path)
    response_filename = image.split('/')[-1].split('.')[0]
    logger.info(f"Response filename for Textract: {response_filename}")
    try:
        print("Starting image preprocessing with OpenCV...")
        logger.info("Starting image preprocessing with OpenCV...")
        import cv2
        import numpy as np
        img = cv2.imread(local_image_path)
        logger.info(f"Original image shape: {img.shape}")
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        logger.info("Converted image to grayscale.")
        # Increase contrast
        alpha = 1.5  # Contrast control
        beta = -30   # Brightness control
        contrast_img = cv2.convertScaleAbs(gray, alpha=alpha, beta=beta)
        logger.info(f"Applied contrast (alpha={alpha}) and brightness (beta={beta}).")
        # Adaptive thresholding to remove faint text
        processed = cv2.adaptiveThreshold(contrast_img, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 21, 15)
        logger.info("Applied adaptive thresholding to suppress reverse-side text.")
        processed_path = f'/tmp/processed_image_{unique_id}.jpg'
        cv2.imwrite(processed_path, processed)
        logger.info(f"Processed image saved to: {processed_path}")
        print(f"Processed image saved to: {processed_path}")
        # Upload processed image to Textract bucket
        processed_s3_key = output_path + '/textractResponse/' + str(response_filename) + '_preprocessed.jpg'
        logger.info(f"Uploading processed image to Textract bucket at: {processed_s3_key}")
        print(f"Uploading processed image to Textract bucket at: {processed_s3_key}")
        s3.upload_file(processed_path, textract_bucket, processed_s3_key)
        # Use processed image for Textract
        image_document = {
            'S3Object' : {
                'Bucket' : textract_bucket,
                'Name' : processed_s3_key
            }
        }
        print("Image preprocessing complete. Using processed image for Textract.")
        logger.info("Image preprocessing complete. Using processed image for Textract.")
    except Exception as e:
        logger.error(f'Image preprocessing failed: {e}')
        print(f'Image preprocessing failed: {e}')
        logger.info(f"Image preprocessing failed: {e}. Using original image for Textract.")
        print(f"Image preprocessing failed: {e}. Using original image for Textract.")
        # Fallback to original image in textract bucket (should not happen unless you copy originals there)
        image_document = {
            'S3Object' : {
                'Bucket' : textract_bucket,
                'Name' : image
            }
        }
    return image_document

# Remove call_textract from lambda_handler signature and logic (keep comments/prints)
def lambda_handler(event, context):
    print("Lambda handler started. Processing event...")
    logger.info("Lambda handler started. Processing event...")

    # Skip cleaning /tmp for local run
    if os.environ.get("AWS_EXECUTION_ENV") is not None:
        # Running in AWS Lambda
        clean_tmp()
        print("Cleaned /tmp directory.")
    else:
        print("Skipping /tmp cleanup for local run.")
        logger.info("Skipping /tmp cleanup for local run.")

    print(f"Event received: {event}")
    logger.info(f"Event received: {event}")
    print("Initializing S3 client...")
    logger.info("Initializing S3 client...")
    s3 = boto3.client('s3')
    records = event['Records']
    print(f"Number of records in event: {len(records)}")
    logger.info(f"Number of records in event: {len(records)}")
    # Use textract bucket for all response operations
    textract_bucket = os.getenv("TEXTRACT_BUCKET")   # fallback to your bucket name
    for record in records:
        body = record['body']
        logger.info(f"body: {body}")
        logger.info(type(body))
        res = json.loads(body)
        logger.info(f'Parsed JSON: {res}')
        source_bucket = res['s3']['bucket']['name']
        filename = res["s3"]["object"]["key"]
        response_filename = filename.split('/')[-1].split('.')[0]
        output_path = '/'.join(filename.split('/')[:-2])  
        logger.info(f'filename: {filename}')
        logger.info(f'source_bucket: {source_bucket}')
        logger.info(f'output_path: {output_path}')
        # Extract collection name from the S3 key path (assumes a specific folder structure)
        # Use passed collection_identifier if present, else fallback to extraction
        collectionname = res.get("collection_identifier")
        if not collectionname:
            parts = filename.split('/')[-4]
            collectionname = parts[0] if len(parts) > 1 else "unknown"
        logger.info(f"Collection name: {collectionname}")
        try:
            print("Starting image validation and preprocessing...")
            logger.info("Starting image validation and preprocessing...")
            image = str(filename)
            # Only allow certain image types for Textract
            if not image.lower().endswith(('.png', '.jpg','.jpeg')):
                print("Image extension not valid. Raising error.")
                logger.error("Image extension not valid. Raising error.")
                raise ValueError("Invalid image source")
            logger.info(f"Image to process: {image}")

            # Always check and save responses in textract bucket
            existing_response_key = output_path + '/textractResponse/' + str(response_filename) + '.json'
            try:
                response_obj = s3.get_object(Bucket=textract_bucket, Key=existing_response_key)
                response = json.loads(response_obj['Body'].read())
                print("Loaded existing Textract response from S3.")
                logger.info("Loaded existing Textract response and image from S3.")
                image_document = {
                    'S3Object': {
                        'Bucket': textract_bucket,
                        'Name': image
                    }
                }
            except s3.exceptions.NoSuchKey:
                print("No existing Textract response found. Preprocessing image...")
                logger.info("No existing Textract response found. Preprocessing image...")
                logger.info(f"s3: {s3}, textract_bucket: {textract_bucket}, image: {image}, output_path: {output_path}, response_filename: {response_filename}")
                # Download image from source bucket, preprocess, and upload to textract bucket
                image_document = preprocess_image_for_textract(s3, source_bucket, image, output_path, response_filename, textract_bucket)
                # Update image_document to point to textract bucket
                image_document['S3Object']['Bucket'] = textract_bucket
                image_document['S3Object']['Name'] = output_path + '/textractResponse/' + str(response_filename) + '_preprocessed.jpg'
                response = textract_client.detect_document_text(Document=image_document)
                logger.info(f"Textract response: {response}")

            logger.info("Saving Textract response to S3...")
            # Save the Textract response JSON to S3 for later use
            s3.put_object(
                Bucket=textract_bucket,
                Key=output_path + '/textractResponse/' + str(response_filename) + '.json',
                Body=json.dumps(response),
                Metadata={'collectionname': collectionname}
            )
            logger.info('Successfully saved textract responses')
            print('Successfully saved textract responses')

            logger.info("Parsing Textract response for line and word info...")
            print("Parsing Textract response for line and word info...")
            # Parse the Textract response for line and word info
            json_contents = json.dumps(response)
            line_info, word_info = parseJSON(json_contents, response_filename)
            logger.info(f"Line info: {line_info}")
            logger.info("Saving line information to S3...")
            print("Saving line information to S3...")
            s3.put_object(Bucket= textract_bucket, Key=output_path+'/textractResponse/'+ str(response_filename)+'_line_information.json', Body=json.dumps(line_info),Metadata={'collectionname':collectionname})
            logger.info('Line information successfully saved!')
            print('Line information successfully saved!')
            logger.info("Saving word information to S3...")
            print("Saving word information to S3...")
            s3.put_object(Bucket= textract_bucket, Key=output_path+'/textractResponse/'+ str(response_filename)+'_word_information.json', Body=json.dumps(word_info),Metadata={'collectionname':collectionname})
            logger.info('Word information successfully saved!')
            print('Word information successfully saved!')

        except ClientError as err:
            error_message = "Couldn't analyze image. " + err.response['Error']['Message']
            print(f"ClientError: {error_message}")
            logger.info(f"ClientError: {error_message}")

    print("Lambda handler completed.")
    logger.info("Lambda handler completed.")
    return {
        'statusCode': 200,
        'body': json.dumps('Success!')
    }


if __name__ == "__main__":
    # For local testing: load a test event and run the handler
    import json
    with open("test_event.json") as f:
        test_event = json.load(f)
    lambda_handler(test_event, None)

