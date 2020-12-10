import json
import urllib.parse
import urllib.request
import requests
import boto3
import io
import re
import pandas as pd
from datetime import datetime, timezone
from dateutil.parser import parse
import uuid
import os
from boto3.dynamodb.conditions import Key

# Environment variables
collection_category = os.getenv('Collection_Category')
rights_statement = os.getenv('Rights_Statement')
biblio_citation = os.getenv('Bibliographic_Citation')
rights_holder = os.getenv('Rights_Holder')
region_name = os.getenv('REGION')
collection_table_name = os.getenv('DYNO_Collection_TABLE')
archive_table_name = os.getenv('DYNO_Archive_TABLE')
app_img_root_path = os.getenv('APP_IMG_ROOT_PATH')
noid_scheme = os.getenv('NOID_Scheme')
noid_naa = os.getenv('NOID_NAA')
long_url_path = os.getenv('LONG_URL_PATH')
short_url_path = os.getenv('SHORT_URL_PATH')
api_key = os.getenv('API_KEY')
api_endpoint = os.getenv('API_ENDPOINT')
 
dyndb = boto3.resource('dynamodb', region_name=region_name)
archive_table = dyndb.Table(archive_table_name)
collection_table = dyndb.Table(collection_table_name)

single_value_headers = ['Identifier', 'Title', 'Description', 'Rights']
multi_value_headers = ['Creator', 'Source', 'Subject', 'Coverage', 'Language', 'Type', 'Is Part Of', 'Medium',
                       'Format', 'Related URL', 'Contributor', 'Tags', 'Provenance', 'Identifier2']
old_key_list = ['title', 'description', 'creator', 'source', 'circa', 'start_date', 'end_date', 'subject',
                'belongs_to', 'resource_type', 'location', 'language', 'rights_statement', 'medium',
                'bibliographic_citation', 'rights_holder', 'format', 'related_url', 'contributor', 'tags', 'parent_collection',
                'collection_category', 'item_category', 'collection', 'manifest_url', 'thumbnail_path', 'visibility',
                'create_date', 'modified_date', 'provenance', 'reference', 'repository', 'createdAt', 'updatedAt']
removable_key_list = ['description', 'creator', 'source', 'circa', 'start_date', 'end_date', 'subject',
                      'belongs_to', 'resource_type', 'location', 'language', 'medium', 'format', 'related_url',
                      'contributor', 'tags', 'rights_statement', 'rights_holder', 'bibliographic_citation',
                      'provenance', 'reference', 'repository']
new_key_list = [':t', ':d', ':c', ':s', ':ci', ':st', ':e', ':su', ':bt', ':rt', ':l', ':la', ':rs', ':me', ':bc', ':rh', ':f',
                ':ru', ':ct', ':tg', ':pc', ':cc', ':ic', ':co', ':mu', ':tp', ':v', ':cd', ':m', ':pv', ':rf', ':rp', ':ca', ':ua']
key_list_len = len(old_key_list)
csv_columns_to_attributes = {'Type': 'resource_type', 'Is Part Of': 'belongs_to', 'Related URL': 'related_url', 'Coverage': 'location', 
                             'Rights': 'rights_statement', 'Identifier2': 'repository'}
reversed_attribute_names = {'source': '#s', 'location': '#l', 'language':'#la', 'format':'#f', 'collection': '#c', 'reference': '#rf'}

def lambda_handler(event, context):

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=bucket, Key=key)
        if 'index.csv' in key:
            batch_import_archives(response)
        else:
            batch_import_collections(response)
    except Exception as e:
        print(f"An error occurred importing {key} from bucket {bucket}: {str(e)}")
        raise e
    else:
        return {
            'statusCode': 200,
            'body': json.dumps('Finish metadata import.')
        }

def batch_import_collections(response):

    df = pd.read_csv(io.BytesIO(response['Body'].read()), na_values='NaN', keep_default_na=False, encoding='utf-8', dtype={'Start Date': str, 'End Date': str})

    for idx, row in df.iterrows():
        collection_dict = process_csv_metadata(row, 'Collection')
        if not collection_dict:
            continue
        identifier = collection_dict['identifier'].strip()
        (result, id_val) = query_by_index(collection_table, 'Identifier', identifier)
        if result == 'Duplicated':
            print(f"Error: Duplicated Collection ({identifier}) has been found.")
            break
        elif result == 'Unique':
            update_item_in_table(collection_table, collection_dict, id_val)
        else:
            collection_dict['thumbnail_path'] = app_img_root_path + identifier + "/representative.jpg"
            create_item_in_table(collection_table, collection_dict, "collection")
        print(f"Collection {idx+1} ({identifier}) has been imported.")

def batch_import_archives(response):
    csv_lines = response['Body'].read().decode('utf-8').split()
    line_count = 0
    for line in csv_lines:
        csv_row = line.split(',')
        # get path to archive metadata csv file
        metadata_csv = csv_row[0].strip()
        csv_path = app_img_root_path + metadata_csv
        # get identifiers with corresponding paths
        archive_identifiers = [i.strip() for i in csv_row[1:]]

        # process archive metadata csv
        df = pd.read_csv(csv_path, na_values='NaN', keep_default_na=False, encoding='utf-8', dtype={'Start Date': str, 'End Date': str})
        for idx, row in df.iterrows():
            archive_dict = process_csv_metadata(row, 'Archive')
            if ('identifier' not in archive_dict.keys()) and ('title' not in archive_dict.keys()):
                break
            elif ('identifier' not in archive_dict.keys()) or ('title' not in archive_dict.keys()):
                continue
            identifier = archive_dict['identifier'].strip()
            matching_parent_paths = [path for path in archive_identifiers if path.endswith(identifier)]
            if len(matching_parent_paths) == 1:
                parent_collections = matching_parent_paths[0].split('/')
                parent_collections.pop()
                if len(parent_collections) > 0:
                    parent_collection_ids = create_sub_collections(parent_collections)
            else:
                print(f"Wrong archive path invloving {identifier} occurred processing {csv_path}")
                continue
            archive_dict['manifest_url'] = app_img_root_path +  matching_parent_paths[0].strip() + '/manifest.json'
            json_url = urllib.request.urlopen(archive_dict['manifest_url'])
            archive_dict['thumbnail_path'] = json.loads(json_url.read())["thumbnail"]["@id"]
            archive_dict['parent_collection'] = parent_collection_ids
            if len(parent_collection_ids) > 0:
                archive_dict['collection'] = parent_collection_ids[0]

            (result, id_val) = query_by_index(archive_table, 'Identifier', identifier)
            if result == 'Duplicated':
                print(f"Error: Duplicated Archive ({identifier}) has been found.")
                break
            elif result == 'Unique':
                update_item_in_table(archive_table, archive_dict, id_val)
            else:
                create_item_in_table(archive_table, archive_dict, "archive")
            print(f"Archive {idx+1} ({identifier}) has been imported.")
        line_count += 1
        print(f"{line_count}: Archive Metadata ({csv_path}) has been processed.")

def create_item_in_table(table, attr_dict, item_type):
    attr_dict['id'] = str(uuid.uuid4())
    short_id = mint_NOID()
    if short_id:
        attr_dict['custom_key'] = noid_scheme + noid_naa + "/" + short_id
    now = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
    attr_dict['create_date'] = now
    attr_dict['modified_date'] = now
    utc_now = utcformat(datetime.now())
    attr_dict['createdAt'] = utc_now
    attr_dict['updatedAt'] = utc_now
    response = table.put_item(
        Item=attr_dict
    )
    print('PutItem succeeded:')
    print(response)
    if short_id:
        # after NOID is created and item is inserted, update long_url and short_url through API
        long_url = long_url_path + item_type + "/" + short_id
        short_url = short_url_path + noid_scheme + noid_naa + "/" + short_id
        update_NOID(long_url, short_url, short_id, now)

def update_item_in_table(table, attr_dict, key_val):
    del attr_dict['identifier']
    now = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
    attr_dict['modified_date'] = now
    utc_now = utcformat(datetime.now())
    attr_dict['updatedAt'] = utc_now
    response = update_remove_attr_from_table(table, attr_dict, key_val)
    print(response)

def utcformat(dt, timespec='milliseconds'):
    # convert datetime to string in UTC format (YYYY-mm-ddTHH:MM:SS.mmmZ)
    iso_str = dt.astimezone(timezone.utc).isoformat('T', timespec)
    return iso_str.replace('+00:00', 'Z')

def update_remove_attr_from_table(item_table, item_dict, id_val):
    update_expression = 'SET'
    update_dict = {}
    update_names = {}
    for i in range(key_list_len):
        update_expression += update_attr_dict(item_dict, update_dict, old_key_list[i], new_key_list[i], update_names)
    if bool(update_names):
        response = item_table.update_item(
            Key={
                'id': id_val
            },
            UpdateExpression=update_expression.rstrip(','),
            ExpressionAttributeNames=update_names,
            ExpressionAttributeValues=update_dict,
            ReturnValues="UPDATED_NEW"
        )
    else:
        response = item_table.update_item(
            Key={
                'id': id_val
            },
            UpdateExpression=update_expression.rstrip(','),
            ExpressionAttributeValues=update_dict,
            ReturnValues="UPDATED_NEW"
        )
    print("UpdateItem succeeded:")
    remove_names = {}
    remove_expression = remove_attr_dict(item_dict, remove_names)
    if remove_expression != "REMOVE":
        if bool(remove_names):
            response = item_table.update_item(
                Key={
                    'id': id_val
                },
                UpdateExpression=remove_expression.rstrip(','),
                ExpressionAttributeNames=remove_names,
                ReturnValues="UPDATED_NEW"
            )
        else:
            response = item_table.update_item(
                Key={
                    'id': id_val
                },
                UpdateExpression=remove_expression.rstrip(','),
                ReturnValues="UPDATED_NEW"
            )
    print("Remove Attributes succeeded:")
    return response

def process_csv_metadata(data_row, item_type):

    attr_dict = {}
    for items in data_row.iteritems():
        if items[0] and items[1]:
            set_attribute(attr_dict, items[0], items[1])
    if ('identifier' not in attr_dict.keys()) or ('title' not in attr_dict.keys()):
        print(f"Missing required attribute in this row!")
    else:
        set_attributes_from_env(attr_dict, item_type)
    return attr_dict

def set_attributes_from_env(attr_dict, item_type):
    if collection_category == 'IAWA':
        attr_dict['rights_statement'] = rights_statement_with_title(attr_dict['title'])
        attr_dict['bibliographic_citation'] = biblio_citation_with_title(attr_dict['title'])
        attr_dict['rights_holder'] = rights_holder
    if item_type == 'Collection':
        attr_dict['collection_category'] = collection_category
        if 'visibility' not in attr_dict.keys():
            attr_dict['visibility'] = False
    elif item_type == 'Archive':
        attr_dict['item_category'] = collection_category
        attr_dict['visibility'] = True

def set_attribute(attr_dict, attr, value):
    lower_attr = attr.lower().replace(' ', '_')
    if attr == 'Circa':
        if value == 'Yes':
            attr_dict[lower_attr] = 'Circa '
    elif attr == 'Visibility':
        if str(value).lower() == 'true':
            attr_dict[lower_attr] = True
        else:
            attr_dict[lower_attr] = False
    elif attr == 'Start Date':
        # value = value.strip()
        # # e.g., c. 1940s
        # if re.search(r'^(c\. )[0-9]{4}s$', value):
        #     attr_dict['circa'] = 'Circa '
        #     start_date = value[3:7]
        #     attr_dict[lower_attr] = start_date
        # elif re.search(r'^(c\. )', value):
        #     attr_dict['circa'] = 'Circa '
        #     attr_dict[lower_attr] = value[3:]
        # else:
        #     attr_dict[lower_attr] = value
        # print_invalid_date(attr_dict, lower_attr)
        attr_dict[lower_attr] = value.strip()
    elif attr == 'End Date':
        attr_dict[lower_attr] = value.strip()
        # print_invalid_date(attr_dict, lower_attr)
    elif attr == 'Parent Collection':
        parent_collection_ids = []
        parent_identifiers = value.split('~')
        for identifier in parent_identifiers:
            (result, id_val) = query_by_index(collection_table, 'Identifier', identifier)
            if result == 'Unique':
                parent_collection_ids.append(id_val)
        if parent_collection_ids:
            attr_dict[lower_attr] = parent_collection_ids
    elif attr == 'Thumbnail Path':
        attr_dict[lower_attr] = app_img_root_path + value.strip() + "/representative.jpg"
    else:
        if attr in csv_columns_to_attributes:
            lower_attr = csv_columns_to_attributes[attr]
        extracted_value = extract_attribute(attr, value)
        if extracted_value:
            attr_dict[lower_attr] = extracted_value

def print_invalid_date(attr_dict, attr):
    try:
        parsed_date = parse(attr_dict[attr])
        # dates in Elasticsearch are formatted, e.g. "2015-01-01" or "2015/01/01 12:10:30"
        attr_dict[attr] = parsed_date.strftime("%Y/%m/%d")
        print(f"Valid date format: {attr_dict[attr]} for {attr}")
    except ValueError:
        print(f"Error - Unknown date format: {attr_dict[attr]} for {attr}")
        del attr_dict[attr]
    except OverflowError:
        print(f"Error - Invalid date range: {attr_dict[attr]} for {attr}")
        del attr_dict[attr]
    except:
        print(f"Error - Unexpect error: {attr_dict[attr]} for {attr}")
        del attr_dict[attr]

def query_by_index(table, index_name, value):
    index_key = index_name.lower()
    attributes = table.query(
        IndexName=index_name,
        KeyConditionExpression=Key(index_key).eq(value)
    )
    if len(attributes['Items']) > 1:
        print(f"Duplicated {index_name} ({value}) found in {table}.")
        return ('Duplicated', None)
    elif len(attributes['Items']) == 1:
        attributes = attributes['Items'][0]
        id_val = attributes['id']
        return ('Unique', id_val)
    else:
        return ('NotFound', None)

def update_attr_dict(attr_dict, update_attr_dict, old_attr, new_attr, attr_names):
    update_exp = ""
    if old_attr in attr_dict.keys():
        if attr_dict[old_attr] or old_attr == "visibility":
            update_attr_dict[new_attr] = attr_dict[old_attr]
            if old_attr in reversed_attribute_names:
                new_key = reversed_attribute_names[old_attr]
                attr_names[new_key] = old_attr
                update_exp = ' ' + new_key + '=' + new_attr + ','
            else:
                update_exp = ' ' + old_attr + '=' + new_attr + ','
        else:
            del attr_dict[old_attr]
    return update_exp

def remove_attr_dict(attr_dict, attr_names):
    remove_exp = "REMOVE"
    for old_attr in removable_key_list:
        if old_attr not in attr_dict.keys():
            if old_attr in reversed_attribute_names:
                new_key = reversed_attribute_names[old_attr]
                attr_names[new_key] = old_attr
                remove_exp += " " + new_key + ","
            else:
                remove_exp += " " + old_attr + ","
    return remove_exp

def rights_statement_with_title(title):
    index_r_s = rights_statement.find('must')
    return rights_statement[:index_r_s] + title + ' ' + rights_statement[index_r_s:]

def biblio_citation_with_title(title):
    index_b_c = biblio_citation.find('- Special')
    return biblio_citation[:index_b_c] + title + ' ' + biblio_citation[index_b_c:]

def extract_attribute(header, value):
    if header in single_value_headers:
        return value
    elif header in multi_value_headers:
        return value.split('~')

def create_sub_collections(parent_collections):
    parent_collection_ids = None
    for idx in range(len(parent_collections)):
        if idx == 0:
            title = parent_collections[idx]
            identifier = parent_collections[idx]
        else:
            title = parent_collections[idx]
            identifier += "_" + parent_collections[idx]

        collection_dict = {}
        collection_dict['title'] = title
        collection_dict['identifier'] = identifier
        set_attributes_from_env(collection_dict, 'Collection')
        collection_dict['visibility'] = True
        if parent_collection_ids:
            collection_dict['parent_collection'] = parent_collection_ids

        (result, id_val) = query_by_index(collection_table, 'Identifier', identifier)
        if result == 'Duplicated':
            print(f"Error: Duplicated Collection ({identifier}) has been found.")
            break
        elif result == 'Unique':
            print(f"Collection {identifier} exists!")
            parent_collection_ids = [id_val]
        else:
            create_item_in_table(collection_table, collection_dict, "collection")
            print('PutItem succeeded:')
            print(f"collection {identifier} has been created.")
            parent_collection_ids = [collection_dict['id']]
    return parent_collection_ids

def mint_NOID():
    headers = { 'x-api-key': api_key }
    url = api_endpoint + 'mint'
    response = requests.get(url, headers=headers)
    print(f"mint_NOID {response.text}")
    if response.status_code == 200:
        res_message = (response.json())['message']
        start_idx = res_message.find('New NOID: ') + len('New NOID: ')
        end_idx = res_message.find(' is created.', start_idx)
        return res_message[start_idx:end_idx]
    else:
        return None

def update_NOID(long_url, short_url, noid, create_date):
    headers = { 'x-api-key': api_key }
    body = "long_url=" + long_url + "&short_url=" + short_url + "&noid=" + noid + "&create_date=" + create_date
    url = api_endpoint + 'update'
    response = requests.post(url, data=body, headers=headers)
    print(f"update_NOID: {response.text}")
