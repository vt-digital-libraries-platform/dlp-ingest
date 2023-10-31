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

DUPLICATED = "Duplicated"

class GenericTypeMetadata():
  def __init__(self, env, headers_keys, metadata_filename, metadata):
    self.env = env
    self.metadata_filename = metadata_filename
    self.metadata = metadata
    self.headers_keys = headers_keys
    self.headers_keys['old_key_list_len'] = len(self.headers_keys['old_key_list'])

    try:
      dyndb = boto3.resource('dynamodb', region_name=self.env["region_name"])
      self.env["archive_table"] = dyndb.Table(self.env["archive_table_name"])
      self.env["collection_table"] = dyndb.Table(self.env["collection_table_name"])
      self.env["collectionmap_table"] = dyndb.Table(self.env["collectionmap_table_name"])
    except Exception as e:
      print(f"An error occurred: {str(e)}")
      raise e
      


  def ingest(self):
    pass


  def csv_to_dataframe(self, csv_path):
    df = pd.read_csv(
        csv_path,
        na_values='NaN',
        keep_default_na=False,
        encoding='utf-8',
        dtype={
            'Start Date': str,
            'End Date': str})

    df = self.map_dublin_headers_to_schema_keys(df)
    return df
  

  def map_dublin_headers_to_schema_keys(self, records):
    df = records.rename(columns=self.headers_keys['dublin_to_key_map'])
    return df
  

  def find_and_update(self, table, attr_dict, item_type, index):
    identifier = attr_dict['identifier']
    items = self.query_by_index(table, 'Identifier', identifier)
    if len(items) > 1:
        print(f"Error: Duplicated Identifier ({identifier}) found in {table}.")
        return DUPLICATED
    elif len(items) == 1:
        self.update_item_in_table(table, attr_dict, items[0]['id'])
    else:
        self.create_item_in_table(table, attr_dict, item_type)
    print(f"Archive {index+1} ({identifier}) has been imported.")


  def create_item_in_table(self, table, attr_dict, item_type):
    attr_id = str(uuid.uuid4())
    attr_dict['id'] = attr_id
    if item_type == 'Collection':
        if 'heirarchy_path' in attr_dict:
            attr_dict['heirarchy_path'].append(attr_id)
        else:
            attr_dict['heirarchy_path'] = [attr_id]

    short_id = self.mint_NOID()
    if short_id:
        attr_dict['custom_key'] = self.env["noid_scheme"] + self.env["noid_naa"] + "/" + short_id
    now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    utc_now = self.utcformat(datetime.now())
    attr_dict['createdAt'] = utc_now
    attr_dict['updatedAt'] = utc_now
    table.put_item(
        Item=attr_dict
    )
    print(f"PutItem succeeded: {attr_dict['identifier']}")
    if short_id:
        # after NOID is created and item is inserted, update long_url and
        # short_url through API
        long_url = self.env["long_url_path"] + item_type.lower() + "/" + short_id
        short_url = self.env["short_url_path"] + self.env["noid_scheme"] + self.env["noid_naa"] + "/" + short_id
        self.update_NOID(long_url, short_url, short_id, now)
    return attr_id
  

  def update_item_in_table(self, table, attr_dict, key_val):
    del attr_dict['identifier']
    attr_dict['create_date'] = None
    attr_dict['modified_date'] = None
    attr_dict['circa'] = None
    utc_now = self.utcformat(datetime.now())
    attr_dict['updatedAt'] = utc_now
    return self.update_remove_attr_from_table(table, attr_dict, key_val)
  

  def utcformat(self, dt, timespec='milliseconds'):
    # convert datetime to string in UTC format (YYYY-mm-ddTHH:MM:SS.mmmZ)
    iso_str = dt.astimezone(timezone.utc).isoformat('T', timespec)
    return iso_str.replace('+00:00', 'Z')
  

  def update_remove_attr_from_table(self, item_table, item_dict, id_val):
    update_expression = 'SET'
    update_dict = {}
    update_names = {}
    for i in range(self.headers_keys['old_key_list_len']):
        update_expression += self.update_attr_dict(item_dict,
                                              update_dict,
                                              self.headers_keys['old_key_list'][i],
                                              self.headers_keys['new_key_list'][i],
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
    remove_expression = self.remove_attr_dict(item_dict, remove_names)
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
  

  def process_csv_metadata(self, data_row, item_type):
    attr_dict = {}
    for items in data_row.iteritems():
        if items[0].strip() and str(items[1]).strip():
            self.set_attribute(attr_dict, items[0].strip(), str(items[1]).strip())
    if ('identifier' not in attr_dict.keys()) or (
            'title' not in attr_dict.keys()):
        attr_dict = None
        print(f"Missing required attribute in this row!")
    else:
        self.set_attributes_from_env(attr_dict, item_type)
    return attr_dict
  

  def set_attributes_from_env(self, attr_dict, item_type):
    if item_type == 'Collection':
        attr_dict['collection_category'] = self.env["collection_category"]
        if 'visibility' not in attr_dict.keys():
            attr_dict['visibility'] = False
    elif item_type == 'Archive':
        attr_dict['item_category'] = self.env["collection_category"]
        attr_dict['visibility'] = True


  def set_attribute(self, attr_dict, attr, value):
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
        self.print_index_date(attr_dict, value, lower_attr)
    elif attr == 'parent_collection':
        items = self.query_by_index(self.env["collection_table"], 'identifier', value)
        if len(items) == 1:
            parent_collection_id = items[0]['id']
            attr_dict['heirarchy_path'] = items[0]['heirarchy_path']
            attr_dict[lower_attr] = [parent_collection_id]
    elif attr == 'thumbnail_path':
        attr_dict[lower_attr] = self.env["app_img_root_path"] + \
            value + '/representative.jpg'
    elif attr == 'filename':
        if value.endswith('.pdf') or value.endswith('.jpg'):
            attr_dict['thumbnail_path'] = self.env["app_img_root_path"] + \
                'thumbnail/' + value.replace('.pdf', '.jpg')
            attr_dict['manifest_url'] = self.env["app_img_root_path"] + 'pdf/' + value
        elif 'video.vt.edu/media' in value:
            thumbnail = value.split('_')[1]
            attr_dict['thumbnail_path'] = self.env["app_img_root_path"] + \
                'thumbnail/' + thumbnail + '.png'
            attr_dict['manifest_url'] = value
    else:
        extracted_value = self.extract_attribute(attr, value)
        if extracted_value:
            attr_dict[lower_attr] = extracted_value


  def print_index_date(self, attr_dict, value, attr):
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

  
  def query_by_index(self, table, index_name, value):
    index_key = index_name.lower()
    attributes = table.query(
        IndexName=index_name,
        KeyConditionExpression=Key(index_key).eq(value)
    )
    return attributes['Items']
  

  def get_collection(self, collection_id):
    ret_val = None
    try:
        response = self.env["collection_table"].query(
            KeyConditionExpression=Key('id').eq(collection_id),
            Limit=1
        )

        ret_val = response['Items'][0]
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        raise e
    return ret_val
  

  def update_attr_dict(
        self,
        attr_dict,
        updated_attr_dict,
        old_attr,
        new_attr,
        attr_names):
    update_exp = ""
    if old_attr in attr_dict.keys():
        if attr_dict[old_attr] or old_attr == "visibility":
            updated_attr_dict[new_attr] = attr_dict[old_attr]
            if old_attr in self.headers_keys['reversed_attribute_names']:
                new_key = self.headers_keys['reversed_attribute_names'][old_attr]
                attr_names[new_key] = old_attr
                update_exp = ' ' + new_key + '=' + new_attr + ','
            else:
                update_exp = ' ' + old_attr + '=' + new_attr + ','
        else:
            del attr_dict[old_attr]
    return update_exp
  

  def remove_attr_dict(self, attr_dict, attr_names):
    remove_exp = "REMOVE"
    for old_attr in self.headers_keys['removable_key_list']:
        if old_attr not in attr_dict.keys():
            if old_attr in self.headers_keys['reversed_attribute_names']:
                new_key = self.headers_keys['reversed_attribute_names'][old_attr]
                attr_names[new_key] = old_attr
                remove_exp += " " + new_key + ","
            else:
                remove_exp += " " + old_attr + ","
    return remove_exp
  

  def extract_attribute(self, header, value):
    if header in self.headers_keys['single_value_headers']:
        return value
    elif header in self.headers_keys['multi_value_headers']:
        return value.split('||')
    

  def create_sub_collections(self, parent_collections):
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
        self.set_attributes_from_env(collection_dict, 'Collection')
        collection_dict['visibility'] = True
        if parent_id is not None:
          collection_dict['parent_collection'] = [parent_id]
          collection_dict['heirarchy_path'] = heirarchy_list

        items = self.query_by_index(self.env["collection_table"], 'Identifier', identifier)
        if len(items) > 1:
          print(
              f"Error: Duplicated Identifier ({identifier}) found in {self.env['collection_table']}.")
          break
        elif len(items) == 1:
          print(f"Collection {identifier} exists!")
          parent_id = items[0]['id']
          heirarchy_list = items[0]['heirarchy_path']
        else:
          parent_id = self.create_item_in_table(
            self.env["collection_table"], collection_dict, 'Collection')
          print(f"Collection PutItem succeeded: {identifier}")

    if len(heirarchy_list) > 0:
      self.update_collection_map(heirarchy_list[0])
    return [parent_id]
  

  def walk_collection(self, parent):
    custom_key = parent['custom_key'].replace('ark:/53696/', '')
    map_location = {
        'id': parent['id'],
        'name': parent['title'],
        'custom_key': custom_key}
    children = self.get_collection_children(parent['id'])
    if len(children) > 0:
        map_location['children'] = []
        for child in children:
            map_location['children'].append(self.walk_collection(child))
    return map_location
  

  def update_collection_map(self, top_parent_id):
    parent = self.get_collection(top_parent_id)
    if 'parent_collection' not in parent:
      map_obj = self.walk_collection(parent)
      utc_now = self.utcformat(datetime.now())
      if 'collectionmap_id' in parent:
        self.env["collectionmap_table"].update_item(
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
        self.env["collectionmap_table"].put_item(
          Item={
            "id": map_id,
            "map_object": json.dumps(map_obj),
            "collection_id": parent["id"],
            "collectionmap_category": parent["collection_category"],
            "createdAt": utc_now,
            "updatedAt": utc_now
          }
        )

        self.env["collection_table"].update_item(
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


  def get_collection_children(self, parent_id):
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
        response = self.env["collection_table"].scan(**scan_kwargs)
        source_table_items.extend(response['Items'])
        start_key = response.get('LastEvaluatedKey', None)
        done = start_key is None

    except Exception as e:
      print(f"An error occurred: {str(e)}")
      raise e
    return source_table_items
  

  def batch_import_archives(self, modified_metadata=None):
    metadata = modified_metadata if bool(modified_metadata) else self.metadata
    df = self.csv_to_dataframe(io.BytesIO(metadata['Body'].read()))
    for idx, row in df.iterrows():
      archive_dict = self.process_csv_metadata(row, 'Archive')
      if not archive_dict:
        print(f"Error: Archive {idx+1} has failed to be imported.")
        break
      self.find_and_update(self.env['archive_table'], archive_dict, 'Archive', idx)


  def batch_import_archives_from_legacy_manifest_list(self, modified_metadata=None):
    metadata = modified_metadata if bool(modified_metadata) else self.metadata
    csv_lines = metadata['Body'].read().decode('utf-8').split()
    line_count = 0
    for line in csv_lines:
      csv_row = line.split(',')
      # get path to archive metadata csv file
      metadata_csv = csv_row[0].strip()
      csv_path = self.env['app_img_root_path'] + metadata_csv
      # get identifiers with corresponding paths
      archive_identifiers = [i.strip() for i in csv_row[1:]]

      # process archive metadata csv
      df = self.csv_to_dataframe(csv_path)
      for idx, row in df.iterrows():
        archive_dict = self.process_csv_metadata(row, 'Archive')
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
            parent_collection_ids = self.create_sub_collections(
              parent_collections)
        else:
          print(
            f"Wrong archive path invloving {identifier} occurred processing {csv_path}")
          continue
        archive_dict['manifest_url'] = self.env['app_img_root_path'] + \
          matching_parent_paths[0].strip() + '/manifest.json'
        json_url = urllib.request.urlopen(archive_dict['manifest_url'])
        archive_dict['thumbnail_path'] = json.loads(json_url.read())[
          "thumbnail"]["@id"]
        archive_dict['parent_collection'] = parent_collection_ids
        if len(parent_collection_ids) > 0:
          archive_dict['collection'] = parent_collection_ids[0]
          parent_collection = self.get_collection(parent_collection_ids[0])
          archive_dict['heirarchy_path'] = parent_collection['heirarchy_path']

        self.find_and_update(self.env['archive_table'], archive_dict, 'Archive', idx)
      line_count += 1
    print(f"{line_count}: Archive Metadata ({csv_path}) has been processed.")


  def batch_import_collections(self, modified_metadata=None):
    metadata = modified_metadata if bool(modified_metadata) else self.metadata
    df = self.csv_to_dataframe(io.BytesIO(metadata['Body'].read()))

    for idx, row in df.iterrows():
        collection_dict = self.process_csv_metadata(row, 'Collection')
        if not collection_dict:
            print(f"Error: Collection {idx+1} has failed to be imported.")
            break
        identifier = collection_dict['identifier']
        items = self.query_by_index(self.env['collection_table'], 'Identifier', identifier)
        if len(items) > 1:
            print(
                f"Error: Duplicated Identifier ({identifier}) found in {self.env['collection_table']}.")
            break
        elif len(items) == 1:
            if 'heirarchy_path' in collection_dict:
                collection_dict['heirarchy_path'].append(items[0]['id'])
            self.update_item_in_table(
                self.env['collection_table'],
                collection_dict,
                items[0]['id'])
        else:
            collection_dict['thumbnail_path'] = self.env['app_img_root_path'] + \
                identifier + "/representative.jpg"
            self.create_item_in_table(
                self.env['collection_table'],
                collection_dict,
                'Collection')
        if 'heirarchy_path' in collection_dict:
            self.update_collection_map(collection_dict['heirarchy_path'][0])
        print(f"Collection {idx+1} ({identifier}) has been imported.")