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

# Environment variables
collection_category = os.getenv('Collection_Category')
region_name = os.getenv('REGION')
collection_table_name = os.getenv('DYNO_Collection_TABLE')
archive_table_name = os.getenv('DYNO_Archive_TABLE')
collectionmap_table_name = os.getenv('DYNO_Collectionmap_TABLE')
app_img_root_path = os.getenv('APP_IMG_ROOT_PATH')
noid_scheme = os.getenv('NOID_Scheme')
noid_naa = os.getenv('NOID_NAA')
long_url_path = os.getenv('LONG_URL_PATH')
short_url_path = os.getenv('SHORT_URL_PATH')
api_key = os.getenv('API_KEY')
api_endpoint = os.getenv('API_ENDPOINT')


try:
    dyndb = boto3.resource('dynamodb', region_name=region_name)
    archive_table = dyndb.Table(archive_table_name)
    collection_table = dyndb.Table(collection_table_name)
    collectionmap_table = dyndb.Table(collectionmap_table_name)

except Exception as e:
    print(f"An error occurred: {str(e)}")
    raise e

single_value_headers = [
    'bibliographic_citation',
    'circa',
    'display_date',
    'end_date',
    'explicit',
    'explicit_content',
    'extent',
    'identifier',
    'rights_holder',
    'rights_statement',
    'start_date',
    'title']
multi_value_headers = [
    'alternative',
    'basis_of_record',
    'belongs_to',
    'contributor',
    'conforms_to',
    'coverage',
    'created',
    'creator',
    'date',
    'description',
    'format',
    'has_format',
    'has_part',
    'has_version',
    'is_format_of',
    'is_version_of',
    'language',
    'license',
    'location',
    'medium',
    'other_identifier',
    'provenance',
    'publisher',
    'reference',
    'related_url',
    'repository',
    'resource_type',
    'source',
    'subject',
    'tags',
    'temporal']
old_key_list = [
    'title',
    'description',
    'creator',
    'source',
    'circa',
    'start_date',
    'end_date',
    'subject',
    'belongs_to',
    'resource_type',
    'location',
    'language',
    'rights_statement',
    'medium',
    'bibliographic_citation',
    'rights_holder',
    'format',
    'related_url',
    'contributor',
    'tags',
    'parent_collection',
    'collection_category',
    'item_category',
    'collection',
    'manifest_url',
    'thumbnail_path',
    'visibility',
    'create_date',
    'modified_date',
    'provenance',
    'reference',
    'repository',
    'createdAt',
    'updatedAt',
    'display_date',
    'extent',
    'heirarchy_path',
    'alternative',
    'publisher',
    'date',
    'created',
    'coverage',
    'conforms_to',
    'has_format',
    'has_part',
    'has_version',
    'is_format_of',
    'is_version_of',
    'other_identifier',
    'basis_of_record',
    'temporal',
    'license']
removable_key_list = [
    'description',
    'creator',
    'source',
    'circa',
    'start_date',
    'end_date',
    'subject',
    'belongs_to',
    'resource_type',
    'location',
    'language',
    'medium',
    'format',
    'related_url',
    'contributor',
    'tags',
    'rights_statement',
    'rights_holder',
    'bibliographic_citation',
    'provenance',
    'reference',
    'repository',
    'create_date',
    'modified_date',
    'extent',
    'alternative',
    'publisher',
    'date',
    'created',
    'coverage',
    'conforms_to',
    'has_format',
    'has_part',
    'has_version',
    'is_format_of',
    'is_version_of',
    'other_identifier',
    'basis_of_record',
    'temporal',
    'license']
new_key_list = [
    ':t',
    ':d',
    ':c',
    ':s',
    ':ci',
    ':st',
    ':e',
    ':su',
    ':bt',
    ':rt',
    ':l',
    ':la',
    ':rs',
    ':me',
    ':bc',
    ':rh',
    ':f',
    ':ru',
    ':ct',
    ':tg',
    ':pc',
    ':cc',
    ':ic',
    ':co',
    ':mu',
    ':tp',
    ':v',
    ':cd',
    ':m',
    ':pv',
    ':rf',
    ':rp',
    ':ca',
    ':ua',
    ':dd',
    ':et',
    ':hp',
    ':alt',
    ':pub',
    ':dat',
    ':cre',
    ':cov',
    ':conf',
    ':hasfo',
    ':haspa',
    ':hasve',
    ':isfo',
    ':isve',
    ':othid',
    ':basofre',
    ':tmpl',
    ':lic']
key_list_len = len(old_key_list)
reversed_attribute_names = {
    'source': '#s',
    'location': '#l',
    'language': '#la',
    'format': '#f',
    'collection': '#c',
    'reference': '#rf',
    'date': '#dat'}

DUPLICATED = "Duplicated"

def local_handler(filename):
    body_encoded = open(filename).read().encode()
    stream = StreamingBody(io.BytesIO(body_encoded),len(body_encoded))
    response = {"Body": stream}

    if 'index.csv' in filename:
        batch_import_archives_with_path(response)
    elif 'collection_metadata.csv' in filename:
        batch_import_collections(response)
    elif 'archive_metadata.csv' in filename:
        batch_import_archives(response)

def lambda_handler(event, context):

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(
        event['Records'][0]['s3']['object']['key'],
        encoding='utf-8')
    try:
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=bucket, Key=key)
        if 'index.csv' in key:
            batch_import_archives_with_path(response)
        elif 'collection_metadata.csv' in key:
            batch_import_collections(response)
        elif 'archive_metadata.csv' in key:
            batch_import_archives(response)
    except Exception as e:
        print(
            f"An error occurred importing {key} from bucket {bucket}: {str(e)}")
        raise e
    else:
        return {
            'statusCode': 200,
            'body': json.dumps('Finish metadata import.')
        }


def csv_to_dataframe(csv_path):
    df = pd.read_csv(
        csv_path,
        na_values='NaN',
        keep_default_na=False,
        encoding='utf-8',
        dtype={
            'Start Date': str,
            'End Date': str})

    df = header_update(df)

    return df


def batch_import_collections(response):

    df = csv_to_dataframe(io.BytesIO(response['Body'].read()))

    for idx, row in df.iterrows():
        collection_dict = process_csv_metadata(row, 'Collection')
        if not collection_dict:
            print(f"Error: Collection {idx+1} has failed to be imported.")
            break
        identifier = collection_dict['identifier']
        items = query_by_index(collection_table, 'Identifier', identifier)
        if len(items) > 1:
            print(
                f"Error: Duplicated Identifier ({identifier}) found in {collection_table}.")
            break
        elif len(items) == 1:
            if 'heirarchy_path' in collection_dict:
                collection_dict['heirarchy_path'].append(items[0]['id'])
            update_item_in_table(
                collection_table,
                collection_dict,
                items[0]['id'])
        else:
            collection_dict['thumbnail_path'] = app_img_root_path + \
                identifier + "/representative.jpg"
            create_item_in_table(
                collection_table,
                collection_dict,
                'Collection')
        if 'heirarchy_path' in collection_dict:
            update_collection_map(collection_dict['heirarchy_path'][0])
        print(f"Collection {idx+1} ({identifier}) has been imported.")


def batch_import_archives(response):

    df = csv_to_dataframe(io.BytesIO(response['Body'].read()))

    for idx, row in df.iterrows():
        archive_dict = process_csv_metadata(row, 'Archive')
        if not archive_dict:
            print(f"Error: Archive {idx+1} has failed to be imported.")
            break
        find_and_update(archive_table, archive_dict, 'Archive', idx)


def batch_import_archives_with_path(response):

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
        df = csv_to_dataframe(csv_path)

        for idx, row in df.iterrows():
            archive_dict = process_csv_metadata(row, 'Archive')
            if not archive_dict:
                print(f"Error: Archive {idx+1} has failed to be imported.")
                break
            identifier = archive_dict['identifier']
            matching_parent_paths = [
                path for path in archive_identifiers if path.endswith(identifier)]
            if len(matching_parent_paths) == 1:
                parent_collections = matching_parent_paths[0].split('/')
                parent_collections.pop()
                if len(parent_collections) > 0:
                    parent_collection_ids = create_sub_collections(
                        parent_collections)
            else:
                print(
                    f"Wrong archive path invloving {identifier} occurred processing {csv_path}")
                continue
            archive_dict['manifest_url'] = app_img_root_path + \
                matching_parent_paths[0].strip() + '/manifest.json'
            json_url = urllib.request.urlopen(archive_dict['manifest_url'])
            archive_dict['thumbnail_path'] = json.loads(json_url.read())[
                "thumbnail"]["@id"]
            archive_dict['parent_collection'] = parent_collection_ids
            if len(parent_collection_ids) > 0:
                archive_dict['collection'] = parent_collection_ids[0]
                parent_collection = get_collection(parent_collection_ids[0])
                archive_dict['heirarchy_path'] = parent_collection['heirarchy_path']

            find_and_update(archive_table, archive_dict, 'Archive', idx)
        line_count += 1
        print(f"{line_count}: Archive Metadata ({csv_path}) has been processed.")


def header_update(records):

    df = records.rename(columns={
        'dcterms.alternative': 'alternative',
        'dcterms.bibliographicCitation': 'bibliographic_citation',
        'dcterms.conformsTo': 'conforms_to',
        'dcterms.contributor': 'contributor',
        'dcterms.coverage': 'coverage',
        'dcterms.created': 'created',
        'dcterms.creator': 'creator',
        'dcterms.date': 'date',
        'dcterms.description': 'description',
        'dcterms.extent': 'extent',
        'dcterms.format': 'format',
        'dcterms.hasFormat': 'has_format',
        'dcterms.hasPart': 'has_part',
        'dcterms.hasVersion': 'has_version',
        'dcterms.identifier': 'identifier',
        'dcterms.isFormatOf': 'is_format_of',
        'dcterms.isPartOf': 'belongs_to',
        'dcterms.isVersionOf': 'is_version_of',
        'dcterms.language': 'language',
        'dcterms.license': 'license',
        'dcterms.medium': 'medium',
        'dcterms.provenance': 'provenance',
        'dcterms.publisher': 'publisher',
        'dcterms.references': 'reference',
        'dcterms.relation': 'related_url',
        'dcterms.rights': 'rights_statement',
        'dcterms.source': 'source',
        'dcterms.spatial': 'location',
        'dcterms.subject': 'subject',
        'dcterms.rightsHolder': 'rights_holder',
        'dcterms.temporal': 'temporal',
        'dcterms.title': 'title',
        'dcterms.type': 'resource_type'})

    return df


def find_and_update(table, attr_dict, item_type, index):
    identifier = attr_dict['identifier']
    items = query_by_index(table, 'Identifier', identifier)
    if len(items) > 1:
        print(f"Error: Duplicated Identifier ({identifier}) found in {table}.")
        return DUPLICATED
    elif len(items) == 1:
        update_item_in_table(table, attr_dict, items[0]['id'])
    else:
        create_item_in_table(table, attr_dict, item_type)

    print(f"Archive {index+1} ({identifier}) has been imported.")


def create_item_in_table(table, attr_dict, item_type):
    attr_id = str(uuid.uuid4())
    attr_dict['id'] = attr_id
    if item_type == 'Collection':
        if 'heirarchy_path' in attr_dict:
            attr_dict['heirarchy_path'].append(attr_id)
        else:
            attr_dict['heirarchy_path'] = [attr_id]

    short_id = mint_NOID()
    if short_id:
        attr_dict['custom_key'] = noid_scheme + noid_naa + "/" + short_id
    now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    utc_now = utcformat(datetime.now())
    attr_dict['createdAt'] = utc_now
    attr_dict['updatedAt'] = utc_now
    table.put_item(
        Item=attr_dict
    )
    print(f"PutItem succeeded: {attr_dict['identifier']}")
    if short_id:
        # after NOID is created and item is inserted, update long_url and
        # short_url through API
        long_url = long_url_path + item_type.lower() + "/" + short_id
        short_url = short_url_path + noid_scheme + noid_naa + "/" + short_id
        update_NOID(long_url, short_url, short_id, now)
    return attr_id


def update_item_in_table(table, attr_dict, key_val):
    del attr_dict['identifier']
    attr_dict['create_date'] = None
    attr_dict['modified_date'] = None
    attr_dict['circa'] = None
    utc_now = utcformat(datetime.now())
    attr_dict['updatedAt'] = utc_now
    return update_remove_attr_from_table(table, attr_dict, key_val)


def utcformat(dt, timespec='milliseconds'):
    # convert datetime to string in UTC format (YYYY-mm-ddTHH:MM:SS.mmmZ)
    iso_str = dt.astimezone(timezone.utc).isoformat('T', timespec)
    return iso_str.replace('+00:00', 'Z')


def update_remove_attr_from_table(item_table, item_dict, id_val):
    update_expression = 'SET'
    update_dict = {}
    update_names = {}
    for i in range(key_list_len):
        update_expression += update_attr_dict(item_dict,
                                              update_dict,
                                              old_key_list[i],
                                              new_key_list[i],
                                              update_names)
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
        if items[0].strip() and str(items[1]).strip():
            set_attribute(attr_dict, items[0].strip(), str(items[1]).strip())
    if ('identifier' not in attr_dict.keys()) or (
            'title' not in attr_dict.keys()):
        attr_dict = None
        print(f"Missing required attribute in this row!")
    else:
        set_attributes_from_env(attr_dict, item_type)
    return attr_dict


def set_attributes_from_env(attr_dict, item_type):
    if item_type == 'Collection':
        attr_dict['collection_category'] = collection_category
        if 'visibility' not in attr_dict.keys():
            attr_dict['visibility'] = False
    elif item_type == 'Archive':
        attr_dict['item_category'] = collection_category
        attr_dict['visibility'] = True


def set_attribute(attr_dict, attr, value):
    lower_attr = attr.lower().replace(' ', '_')
    if attr == 'circa':
        if str(value).lower() == 'yes':
            attr_dict[lower_attr] = 'Circa '
    elif attr == 'visibility' or attr == 'explicit_content' or attr == 'explicit':
        if str(value).lower() == 'true':
            attr_dict[lower_attr] = True
        else:
            attr_dict[lower_attr] = False
    elif attr == 'start_date' or attr == 'end_date':
        print_index_date(attr_dict, value, lower_attr)
    elif attr == 'parent_collection':
        items = query_by_index(collection_table, 'Identifier', value)
        if len(items) == 1:
            parent_collection_id = items[0]['id']
            attr_dict['heirarchy_path'] = items[0]['heirarchy_path']
            attr_dict[lower_attr] = [parent_collection_id]
    elif attr == 'thumbnail_path':
        attr_dict[lower_attr] = app_img_root_path + \
            value + '/representative.jpg'
    elif attr == 'filename':
        if value.endswith('.pdf') or value.endswith('.jpg'):
            attr_dict['thumbnail_path'] = app_img_root_path + \
                'thumbnail/' + value.replace('.pdf', '.jpg')
            attr_dict['manifest_url'] = app_img_root_path + 'pdf/' + value
        elif 'video.vt.edu/media' in value:
            thumbnail = value.split('_')[1]
            attr_dict['thumbnail_path'] = app_img_root_path + \
                'thumbnail/' + thumbnail + '.png'
            attr_dict['manifest_url'] = value
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
    attributes = table.query(
        IndexName=index_name,
        KeyConditionExpression=Key(index_key).eq(value)
    )
    return attributes['Items']


def get_collection(collection_id):
    ret_val = None
    try:
        response = collection_table.query(
            KeyConditionExpression=Key('id').eq(collection_id),
            Limit=1
        )

        ret_val = response['Items'][0]
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        raise e
    return ret_val


def update_attr_dict(
        attr_dict,
        update_attr_dict,
        old_attr,
        new_attr,
        attr_names):
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
    return rights_statement[:index_r_s] + \
        title + ' ' + rights_statement[index_r_s:]


def biblio_citation_with_title(title):
    index_b_c = biblio_citation.find('- Special')
    return biblio_citation[:index_b_c] + \
        title + ' ' + biblio_citation[index_b_c:]


def extract_attribute(header, value):
    if header in single_value_headers:
        return value
    elif header in multi_value_headers:
        return value.split('||')


def create_sub_collections(parent_collections):
    parent_id = None
    heirarchy_list = []
    for idx in range(len(parent_collections)):
        if idx == 0:
            identifier = parent_collections[idx]
        else:
            identifier += '_' + parent_collections[idx]

        collection_dict = {}
        collection_dict['title'] = parent_collections[idx]
        collection_dict['identifier'] = identifier
        set_attributes_from_env(collection_dict, 'Collection')
        collection_dict['visibility'] = True
        if parent_id is not None:
            collection_dict['parent_collection'] = [parent_id]
            collection_dict['heirarchy_path'] = heirarchy_list

        items = query_by_index(collection_table, 'Identifier', identifier)
        if len(items) > 1:
            print(
                f"Error: Duplicated Identifier ({identifier}) found in {collection_table}.")
            break
        elif len(items) == 1:
            print(f"Collection {identifier} exists!")
            parent_id = items[0]['id']
            heirarchy_list = items[0]['heirarchy_path']
        else:
            parent_id = create_item_in_table(
                collection_table, collection_dict, 'Collection')
            print(f"Collection PutItem succeeded: {identifier}")

    if len(heirarchy_list) > 0:
        update_collection_map(heirarchy_list[0])
    return [parent_id]


def update_collection_map(top_parent_id):
    parent = get_collection(top_parent_id)
    if 'parent_collection' not in parent:
        map_obj = walk_collection(parent)
        utc_now = utcformat(datetime.now())
        if 'collectionmap_id' in parent:
            collectionmap_table.update_item(
                Key={
                    "id": parent["collectionmap_id"]
                },
                AttributeUpdates={
                    "map_object": {
                        "Value": json.dumps(map_obj),
                        "Action": "PUT"
                    },
                    "collectionmap_category": {
                        "Value": parent["collection_category"],
                        "Action": "PUT"
                    },
                    "updatedAt": {
                        "Value": utc_now,
                        "Action": "PUT"
                    }
                }
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
                    "updatedAt": utc_now
                }
            )

            collection_table.update_item(
                Key={
                    "id": parent["id"]
                },
                AttributeUpdates={
                    "collectionmap_id": {
                        "Value": map_id,
                        "Action": "PUT"
                    }
                }
            )
    else:
        print(f"Error: {parent['identifier']} is not a top level collection")


def get_collection_children(parent_id):
    scan_kwargs = {
        'FilterExpression': Attr('parent_collection').contains(parent_id),
        'ProjectionExpression': "#id, title, custom_key",
        'ExpressionAttributeNames': {"#id": "id"}
    }
    source_table_items = []
    try:
        done = False
        start_key = None
        while not done:
            if start_key:
                scan_kwargs['ExclusiveStartKey'] = start_key
            response = collection_table.scan(**scan_kwargs)
            source_table_items.extend(response['Items'])
            start_key = response.get('LastEvaluatedKey', None)
            done = start_key is None

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        raise e
    return source_table_items


def walk_collection(parent):
    custom_key = parent['custom_key'].replace('ark:/53696/', '')
    map_location = {
        'id': parent['id'],
        'name': parent['title'],
        'custom_key': custom_key}
    children = get_collection_children(parent['id'])
    if len(children) > 0:
        map_location['children'] = []
        for child in children:
            map_location['children'].append(walk_collection(child))

    return map_location


def mint_NOID():
    headers = {'x-api-key': api_key}
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
    headers = {'x-api-key': api_key}
    body = "long_url=" + long_url + "&short_url=" + short_url + \
        "&noid=" + noid + "&create_date=" + create_date
    url = api_endpoint + 'update'
    response = requests.post(url, data=body, headers=headers)
    print(f"update_NOID: {response.text}")



if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 lambda_function.py <filename>")
        sys.exit(1)
    else:
        filename = "".join(sys.argv[1])
        local_handler(filename)
        