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

class MediaType():
  def __init__(self, env, headers_keys):
    self.env = env
    if bool(headers_keys):
      self.single_value_headers = headers_keys['single_value_headers']
      self.multi_value_headers = headers_keys['multi_value_headers']
      self.reversed_attribute_names = headers_keys['reversed_attribute_names']
      self.removable_key_list = headers_keys['removable_key_list']
      self.old_key_list = headers_keys['old_key_list']
      self.key_list_len = len(self.old_key_list)
      self.new_key_list = headers_keys['new_key_list']
      self.dublin_to_key_map = headers_keys['dublin_to_key_map']
    else:
      print(f"Error: initializing MediaType object failed")
      sys.exit(1)


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
    df = records.rename(columns=self.dublin_to_key_map)
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

    short_id = mint_NOID()
    if short_id:
        attr_dict['custom_key'] = noid_scheme + noid_naa + "/" + short_id
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
        long_url = long_url_path + item_type.lower() + "/" + short_id
        short_url = short_url_path + noid_scheme + noid_naa + "/" + short_id
        update_NOID(long_url, short_url, short_id, now)
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
    for i in range(self.key_list_len):
        update_expression += self.update_attr_dict(item_dict,
                                              update_dict,
                                              self.old_key_list[i],
                                              self.new_key_list[i],
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
        attr_dict['collection_category'] = collection_category
        if 'visibility' not in attr_dict.keys():
            attr_dict['visibility'] = False
    elif item_type == 'Archive':
        attr_dict['item_category'] = collection_category
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
        items = self.query_by_index(collection_table, 'identifier', value)
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
            if old_attr in reversed_attribute_names:
                new_key = reversed_attribute_names[old_attr]
                attr_names[new_key] = old_attr
                update_exp = ' ' + new_key + '=' + new_attr + ','
            else:
                update_exp = ' ' + old_attr + '=' + new_attr + ','
        else:
            del attr_dict[old_attr]
    return update_exp
  

  def remove_attr_dict(self, attr_dict, attr_names):
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
  

  def extract_attribute(self, header, value):
    if header in single_value_headers:
        return value
    elif header in multi_value_headers:
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

        items = self.query_by_index(collection_table, 'Identifier', identifier)
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
      self.update_collection_map(heirarchy_list[0])
    return [parent_id]
  

  def update_collection_map(self, top_parent_id):
    parent = self.get_collection(top_parent_id)
    if 'parent_collection' not in parent:
      map_obj = walk_collection(parent)
      utc_now = self.utcformat(datetime.now())
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
        response = collection_table.scan(**scan_kwargs)
        source_table_items.extend(response['Items'])
        start_key = response.get('LastEvaluatedKey', None)
        done = start_key is None

    except Exception as e:
      print(f"An error occurred: {str(e)}")
      raise e
    return source_table_items