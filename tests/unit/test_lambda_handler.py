from distutils import archive_util
from unittest import result
from urllib import response
import boto3
import csv
import io
import pandas as pd
import os
import lambda_function
import unittest

from boto3.dynamodb.conditions import Key, Attr
from botocore.response import StreamingBody
from datetime import datetime
from dateutil.parser import parse
from moto import mock_dynamodb2
from moto import mock_apigateway
from requests.models import Response
from unittest import mock
from unittest.mock import patch


single_value_headers = [
    'Identifier',
    'Title',
    'Description',
    'Rights',
    'Bibliographic Citation',
    'Rights Holder',
    'Extent']
multi_value_headers = [
    'Creator',
    'Source',
    'Subject',
    'Coverage',
    'Language',
    'Type',
    'Is Part Of',
    'Medium',
    'Format',
    'Related URL',
    'Contributor',
    'Tags',
    'Provenance',
    'Identifier2',
    'Reference']

csv_columns_to_attributes = {
    'Type': 'resource_type',
    'Is Part Of': 'belongs_to',
    'Coverage': 'location',
    'Rights': 'rights_statement',
    'Identifier2': 'repository'}

test_file = 'test.csv'
rows = [
    ['Start Date', 'End Date'],
    ['12/01/2022', '12/02/2022'],
]

test_s3_event_single_archive = {
    "Records": [{
        "s3": {
            "bucket": {
                "name": 'vtdlp-dev-test'
            },
            "object": {
                "key": 'single_archive_metadata.csv'
            }
        }
    }]
}

test_s3_event_archive_with_path = {
    "Records": [{
        "s3": {
            "bucket": {
                "name": 'vtdlp-dev-test'
            },
            "object": {
                "key": 'SFD_index.csv'
            }
        }
    }]
}

test_s3_event_collection = {
    "Records": [{
        "s3": {
            "bucket": {
                "name": 'vtdlp-dev-test'
            },
            "object": {
                "key": 'new_collection_metadata.csv'
            }
        }
    }]
}

try:
    dyndb = boto3.resource('dynamodb', region_name="us-east-1")
    archive_table = dyndb.Table("archive_test")
    collection_table = dyndb.Table("collection_test")
    collectionmap_table = dyndb.Table("collectionmap_test")
    mint_table = dyndb.Table("mint_test")

except Exception as e:
    print(f"An error occurred: {str(e)}")
    raise e


class TestLambda(unittest.TestCase):

    def setUp(self):
        with open(test_file, 'w', newline='') as csv_file:
            writer = csv.writer(csv_file, dialect='excel')
            writer.writerows(rows)

    def tearDown(self):
        os.remove(test_file)

    def get_record_by_id(self, table, field, id):
        ret_val = None
        try:
            response = table.query(
                KeyConditionExpression=Key(field).eq(id),
                Limit=1
            )

            ret_val = response['Items']
        except Exception as e:
            print(f"An error occurred: {str(e)}")
            raise e
        return ret_val

    def delete_record_attr(self, table, id, field):

        try:
            table.update_item(
                Key={
                    "id": id
                },
                AttributeUpdates={
                    field: {
                        "Action": "DELETE"
                    }
                }
            )
        except Exception as e:
            print(f"An error occurred: {str(e)}")
            raise e

    def delete_table_record(self, table, field, id):

        try:
            response = table.delete_item(
                Key={
                    field: id
                }
            )
        except Exception as e:
            print(f"An error occurred: {str(e)}")
            raise e
        else:
            return response

    def get_response_from_file(self, file_name):

        with open(file_name, 'r') as file:
            data = file.read()

        body_encoded = data.encode()

        body = StreamingBody(
            io.BytesIO(body_encoded),
            len(body_encoded)
        )

        response = {
            'Body': body
        }

        return response

    def test_utcformat(self):

        # Test: convert datetime to string in UTC format
        # (YYYY-mm-ddTHH:MM:SS.mmmZ)
        test_datetime = datetime.strptime(
            u'2022-1-20T10:41:32.945Z', '%Y-%m-%dT%H:%M:%S.%fZ')
        assert lambda_function.utcformat(
            test_datetime) == "2022-01-20T15:41:32.945Z"

    def test_print_index_date(self):

        lower_attr = "Start Date".lower().replace(' ', '_')
        input_value = "2022-01-20T15:41:32.945Z"
        attr_dict = {}
        attr_dict[lower_attr] = '2022-01-20T15:41:32'

        lambda_function.print_index_date(attr_dict, input_value, lower_attr)
        assert attr_dict[lower_attr] == "2022/01/20"
        print(attr_dict)

        input_value = "unknown_format"
        lambda_function.print_index_date(attr_dict, input_value, lower_attr)
        input_value = "January 1st, 9999999999"
        lambda_function.print_index_date(attr_dict, input_value, lower_attr)

    def test_print_display_date(self):

        lower_attr = "Display_Date".lower().replace(' ', '_')
        input_value = "2022-01-20T15:41:32.945Z"
        attr_dict = {}
        attr_dict[lower_attr] = '2022-01-20T15:41:32'
        lambda_function.print_display_date(attr_dict, input_value, lower_attr)
        assert attr_dict[lower_attr] == "2022-01-20"
        input_value = "unknown_format"
        lambda_function.print_display_date(attr_dict, input_value, lower_attr)
        input_value = "January 1st, 9999999999"
        lambda_function.print_display_date(attr_dict, input_value, lower_attr)

    def test_rights_statement_with_title(self):

        test_value = "Cannery Row, July 28, 1961. Elevation (Ms2008-089)"

        assert lambda_function.rights_statement_with_title(
            test_value) == "\"Permission to publish material from the Cannery Row, July 28, 1961. Elevation (Ms2008-089) must be obtained from University Libraries Special Collections, Virginia Tech.\""

    def test_biblio_citation_with_title(self):

        test_value = "Cannery Row, July 28, 1961. Elevation (Ms2008-089)"

        assert lambda_function.biblio_citation_with_title(
            test_value) == "\"Researchers wishing to cite this collection should include the following information: Cannery Row, July 28, 1961. Elevation (Ms2008-089) - Special Collections, Virginia Polytechnic Institute and State University, Blacksburg, Va.\""

    def test_extract_attribute(self):

        header = "Title"
        test_value = "This is a Title"

        assert lambda_function.extract_attribute(
            header, test_value) == "This is a Title"

        header = "Creator"
        test_value = "Klock, Derek||Roanoke Times"
        assert lambda_function.extract_attribute(
            header, test_value) == ['Klock, Derek', 'Roanoke Times']

    def test_header_update(self):

        df = pd.DataFrame(
            columns=[
                'dcterms.bibliographicCitation',
                'dcterms.contributor',
                'dcterms.coverage',
                'dcterms.created',
                'dcterms.creator',
                'dcterms.date',
                'dcterms.description',
                'dcterms.extent',
                'dcterms.format',
                'dcterms.identifier',
                'dcterms.isPartOf',
                'dcterms.language',
                'dcterms.medium',
                'dcterms.provenance',
                'dcterms.references',
                'dcterms.relation',
                'dcterms.rights',
                'dcterms.source',
                'dcterms.subject',
                'dcterms.rightsHolder',
                'dcterms.title',
                'dcterms.type'])

        result = lambda_function.header_update(df)

        assert "Bibliographic Citation" in list(result.columns)
        assert "Contributor" in list(result.columns)
        assert "Coverage" in list(result.columns)
        assert "Creator" in list(result.columns)
        assert "Description" in list(result.columns)
        assert "End Date" in list(result.columns)
        assert "Extent" in list(result.columns)
        assert "Format" in list(result.columns)
        assert "Identifier" in list(result.columns)
        assert "Is Part Of" in list(result.columns)
        assert "Language" in list(result.columns)
        assert "Medium" in list(result.columns)
        assert "Provenance" in list(result.columns)
        assert "Reference" in list(result.columns)
        assert "Related URL" in list(result.columns)
        assert "Rights" in list(result.columns)
        assert "Rights Holder" in list(result.columns)
        assert "Start Date" in list(result.columns)
        assert "Source" in list(result.columns)
        assert "Subject" in list(result.columns)
        assert "Title" in list(result.columns)
        assert "Type" in list(result.columns)

    def test_set_attribute(self):

        attr_dict = {}

        lambda_function.set_attribute(attr_dict, 'Circa', 'Yes')
        assert attr_dict['circa'] == "Circa "

        lambda_function.set_attribute(attr_dict, 'Visibility', 'True')
        assert attr_dict['visibility']

        lambda_function.set_attribute(
            attr_dict, 'Start Date', '2022-01-20T15:41:32.945Z')
        assert attr_dict['start_date'] == "2022/01/20"

        lambda_function.set_attribute(
            attr_dict, 'End Date', '2022-01-20T15:41:32.945Z')
        assert attr_dict['end_date'] == "2022/01/20"

        lambda_function.set_attribute(
            attr_dict, 'Display_Date', '2022-01-20T15:41:32.945Z')
        assert attr_dict['display_date'] == "2022-01-20"

        lambda_function.set_attribute(
            attr_dict,
            'Parent Collection',
            "Ms2007_009_Roth_Ms2007_009_Box1_Ms2007_009_Folder11")
        assert 'heirarchy_path' in attr_dict

        lambda_function.set_attribute(attr_dict, 'Thumbnail Path', 'tesfile')
        assert attr_dict['thumbnail_path'] == "https://img.cloud.lib.vt.edu/swva/tesfile/representative.jpg"

        lambda_function.set_attribute(attr_dict, 'Filename', 'tesfile.pdf')
        assert attr_dict['thumbnail_path'] == "https://img.cloud.lib.vt.edu/swva/thumbnail/tesfile.jpg"
        assert attr_dict['manifest_url'] == "https://img.cloud.lib.vt.edu/swva/pdf/tesfile.pdf"

        lambda_function.set_attribute(attr_dict, 'Filename', 'tesfile.jpg')
        assert attr_dict['thumbnail_path'] == "https://img.cloud.lib.vt.edu/swva/thumbnail/tesfile.jpg"
        assert attr_dict['manifest_url'] == "https://img.cloud.lib.vt.edu/swva/pdf/tesfile.jpg"

        lambda_function.set_attribute(
            attr_dict, 'Filename', 'https://video.vt.edu/media/1_9eja9h79')
        assert attr_dict['thumbnail_path'] == "https://img.cloud.lib.vt.edu/swva/thumbnail/9eja9h79.png"
        assert attr_dict['manifest_url'] == "https://video.vt.edu/media/1_9eja9h79"

        test_value = "Virginia Tech News"
        lambda_function.set_attribute(attr_dict, 'Is Part Of', test_value)
        assert attr_dict['belongs_to'] == ['Virginia Tech News']

        test_value = "Virginia Tech News||Virginia Tech"
        lambda_function.set_attribute(attr_dict, 'Is Part Of', test_value)
        assert attr_dict['belongs_to'] == [
            'Virginia Tech News', 'Virginia Tech']

    def test_set_attributes_from_env(self):

        attr_dict = {}
        attr_dict['title'] = "Test Title"

        lambda_function.set_attributes_from_env(attr_dict, 'Collection')
        assert attr_dict['collection_category'] == "IAWA"
        assert attr_dict['visibility'] == False

        lambda_function.set_attributes_from_env(attr_dict, 'Archive')
        assert attr_dict['visibility']
        assert attr_dict['rights_statement'] == '"Permission to publish material from the Test Title must be obtained from University Libraries Special Collections, Virginia Tech."'
        assert attr_dict['bibliographic_citation'] == '"Researchers wishing to cite this collection should include the following information: Test Title - Special Collections, Virginia Polytechnic Institute and State University, Blacksburg, Va."'
        assert attr_dict['rights_holder'] == '"VT Libraries"'
        assert attr_dict['item_category'] == "IAWA"

    def test_update_attr_dict(self):

        attr_dict = {}
        attr_dict['title'] = "This is a Title"
        attr_dict['source'] = "This is a Source"
        attr_dict['visibility'] = True
        attr_dict['create_date'] = None
        attr_dict['modified_date'] = None
        attr_dict['subject'] = None
        attr_dict['circa'] = None
        utc_now = lambda_function.utcformat(datetime.now())
        attr_dict['updatedAt'] = utc_now

        attr_names = {}
        update_dict = {}

        result = lambda_function.update_attr_dict(
            attr_dict,
            update_dict,
            lambda_function.old_key_list[0],
            lambda_function.new_key_list[0],
            attr_names)

        assert result == " title=:t,"

        result = lambda_function.update_attr_dict(
            attr_dict,
            update_dict,
            lambda_function.old_key_list[3],
            lambda_function.new_key_list[3],
            attr_names)

        assert result == " #s=:s,"

        result = lambda_function.update_attr_dict(
            attr_dict,
            update_dict,
            "visibility",
            lambda_function.new_key_list[26],
            attr_names)

        assert result == " visibility=:v,"

        result = lambda_function.update_attr_dict(
            attr_dict,
            update_dict,
            lambda_function.old_key_list[7],
            lambda_function.new_key_list[7],
            attr_names)
        assert "subject" not in attr_dict
        assert result == ""

    def test_remove_attr_dict(self):

        attr_dict = {}
        attr_dict['title'] = "This is a Title"
        attr_dict['source'] = "This is a Source"
        attr_dict['description'] = "This is a description"
        attr_dict['visibility'] = True
        attr_dict['create_date'] = None
        attr_dict['modified_date'] = None
        attr_dict['subject'] = None
        attr_names = {}

        result = lambda_function.remove_attr_dict(attr_dict,
                                                  attr_names)
        assert "description" not in result
        assert "#la" in result

    def test_mint_NOID(self):
        result = lambda_function.mint_NOID()
        assert len(result) == 8
        self.delete_table_record(mint_table, "short_id", result)

    def test_update_NOID(self):
        long_url = "https://test-long-cloud.lib.vt.edu"
        short_url = "https://test-short-cloud.lib.vt.edu"
        noid = "7507158m"
        create_date = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        lambda_function.update_NOID(long_url, short_url, noid, create_date)
        result = self.get_record_by_id(mint_table, "short_id", noid)
        assert result[0]['short_url'] == "https://test-short-cloud.lib.vt.edu"
        assert result[0]['created_at'][:16] == create_date[:16]

    def test_process_csv_metadata(self):
        csv_file = 'tests/unit/test_data/hokie_example.csv'
        df = lambda_function.csv_to_dataframe(csv_file)
        df = df.drop('Parent Collection', axis=1)

        archive_dict = lambda_function.process_csv_metadata(
            df.iloc[0], 'Archive')
        assert archive_dict['title'] == "Doris Tinsley Graduation Video"

        df = df.drop('Identifier', axis=1)
        archive_dict = lambda_function.process_csv_metadata(
            df.iloc[0], 'Archive')
        assert archive_dict is None

    def test_csv_to_dataframe(self):
        csv_file = 'tests/unit/test_data/hokie_example.csv'
        df = lambda_function.csv_to_dataframe(csv_file)
        assert df.shape[0] == 179

    def test_get_collection(self):
        restult = lambda_function.get_collection(
            "24ad7228-485d-47a9-9558-c2cff74735b8")
        assert restult['identifier'] == "Ms1998_022_Young_Ms1998_022_Box2_Ms1998_022_B2_Folder19"

    def test_query_by_index(self):

        dyndb = boto3.resource('dynamodb', region_name="us-east-1")
        collection_table = dyndb.Table(
            "Collection-g5nycnwj7zblldz7x6fblmiefu-dev")
        result = lambda_function.query_by_index(
            collection_table,
            'Identifier',
            'Ms1998_022_Young_Ms1998_022_Box2_Ms1998_022_B2_Folder19')
        assert result[0]['id'] == "24ad7228-485d-47a9-9558-c2cff74735b8"

    def test_get_collection_children(self):

        result = lambda_function.get_collection_children(
            "32b171fc-6285-47a0-b636-62a5781d95b2")
        assert result[0]['id'] == "24ad7228-485d-47a9-9558-c2cff74735b8"

        result = lambda_function.get_collection_children(
            "33b171fc-6285-47a0-b636-62a5781d95b2")
        assert result == []

    def test_walk_collection(self):

        # Alberta Pfeiffer Architectural Collection, 1929-1976 (Ms1988-017)
        parent = lambda_function.get_collection(
            "db2e8b79-1909-4864-b27e-66a13d7eccb3")
        result = lambda_function.walk_collection(parent)
        assert result['id'] == "db2e8b79-1909-4864-b27e-66a13d7eccb3"
        assert result['children'][0]['id'] == "073f946b-4d26-4da9-bff0-a0a3606f0015"

    def test_create_item_in_table(self):

        collection_dict = {}
        collection_dict['title'] = "Test Collection"
        collection_dict['identifier'] = "test_collection_identifier"
        collection_dict['visibility'] = True

        result = lambda_function.create_item_in_table(
            collection_table, collection_dict, 'Collection')

        assert len(result) == 36
        record = self.get_record_by_id(collection_table, 'id', result)
        self.delete_table_record(collection_table, "id", result)
        self.delete_table_record(
            mint_table, "short_id", record[0]['custom_key'].split('/')[-1])

    def test_create_sub_collections(self):

        # Duplicate sub-collection
        parent_collections = ['Ms1994_016_Crawford', 'Ms1994_016_Folder1']
        lambda_function.create_sub_collections(parent_collections)

        # Update sub-collection
        parent_collections = ['Ms1997_003_Gottlieb', 'Box2']
        result = lambda_function.create_sub_collections(parent_collections)
        assert result == ['bc7db6e9-138a-4449-8706-477e6929df25']

        # Create sub-collection
        parent_collections = ['Ms1997_003_Gottlieb', 'Box2', 'Folder1']
        result = lambda_function.create_sub_collections(parent_collections)

        record = lambda_function.query_by_index(
            collection_table, 'Identifier', "Ms1997_003_Gottlieb_Box2_Folder1")

        assert "bc7db6e9-138a-4449-8706-477e6929df25" in record[0]['parent_collection']

        self.delete_table_record(collection_table, "id", record[0]['id'])
        self.delete_table_record(
            mint_table, "short_id", record[0]['custom_key'].split('/')[-1])

    def test_batch_import_collections(self):

        duplicated_csv_file = 'tests/unit/test_data/duplicated_collection_metadata.csv'
        response = self.get_response_from_file(duplicated_csv_file)
        lambda_function.batch_import_collections(response)

        invalid_csv_file = 'tests/unit/test_data/invalid_collection_metadata.csv'
        response = self.get_response_from_file(invalid_csv_file)
        lambda_function.batch_import_collections(response)

        update_csv_file = 'tests/unit/test_data/update_collection_metadata.csv'
        response = self.get_response_from_file(update_csv_file)
        utc_now = lambda_function.utcformat(datetime.now())
        lambda_function.batch_import_collections(response)

        record = lambda_function.query_by_index(
            collection_table, 'Identifier', "nonvtsrc")

        assert record[0]['updatedAt'][:16] == utc_now[:16]

        # Create collection: Ms2007_007_Johnson
        new_csv_file = 'tests/unit/test_data/new_collection_metadata.csv'
        response = self.get_response_from_file(new_csv_file)
        lambda_function.batch_import_collections(response)

        record = lambda_function.query_by_index(
            collection_table, 'Identifier', "Ms2007_007_Johnson")

        assert record[0]['identifier'] == "Ms2007_007_Johnson"

        self.delete_table_record(collection_table, "id", record[0]['id'])
        self.delete_table_record(
            collectionmap_table, "id", record[0]['collectionmap_id'])
        self.delete_table_record(
            mint_table, "short_id", record[0]['custom_key'].split('/')[-1])

    def test_batch_import_archives(self):

        invalid_csv_file = 'tests/unit/test_data/invalid_archive_metadata.csv'
        response = self.get_response_from_file(invalid_csv_file)
        lambda_function.batch_import_archives(response)

        # Create archive: Ms2020-004_com001001
        new_csv_file = 'tests/unit/test_data/single_archive_metadata.csv'
        response = self.get_response_from_file(new_csv_file)
        lambda_function.batch_import_archives(response)

        record = lambda_function.query_by_index(
            archive_table, 'Identifier', "Ms2020-004_com001001")

        assert record[0]['identifier'] == "Ms2020-004_com001001"

        self.delete_table_record(archive_table, "id", record[0]['id'])
        self.delete_table_record(
            mint_table, "short_id", record[0]['custom_key'].split('/')[-1])

    def verify_delete_SFD_record(self, id):
        record = lambda_function.query_by_index(
            archive_table, 'Identifier', id)

        assert record[0]['parent_collection'] == [
            "98f839b9-b842-4cd8-9eaf-8eca7d68c63e"]

        self.delete_table_record(archive_table, "id", record[0]['id'])
        self.delete_table_record(
            mint_table, "short_id", record[0]['custom_key'].split('/')[-1])

    def test_batch_import_archives_with_path(self):

        # SWVA records: SFD_index.csv. sfdst001001, sfdst001002, sfdst001003
        new_csv_file = 'tests/unit/test_data/SFD_index.csv'
        response = self.get_response_from_file(new_csv_file)
        lambda_function.batch_import_archives_with_path(response)

        self.verify_delete_SFD_record("sfdst001001")
        self.verify_delete_SFD_record("sfdst001002")
        self.verify_delete_SFD_record("sfdst001003")

    def test_update_collection_map(self):

        utc_now = lambda_function.utcformat(datetime.now())

        # Test update exist "collectionmap_id": "dabe803b-5d95-453d-9361-5051faedfe17"
        # with parent_id "db2e8b79-1909-4864-b27e-66a13d7eccb3"
        lambda_function.update_collection_map(
            "db2e8b79-1909-4864-b27e-66a13d7eccb3")

        map_object = self.get_record_by_id(
            collectionmap_table, "id", "dabe803b-5d95-453d-9361-5051faedfe17")

        assert map_object[0]['updatedAt'][:16] == utc_now[:16]

        # Test create new collectionmap record
        parent_id = "e3263763-bf11-459e-8b5f-974c09d91f6b"
        lambda_function.update_collection_map(
            parent_id)

        colection_record = lambda_function.get_collection(
            parent_id)
        collectionmap_id = self.get_record_by_id(
            collectionmap_table, "id", colection_record['collectionmap_id'])
        assert collectionmap_id[0]['id'] == colection_record['collectionmap_id']

        self.delete_table_record(
            collectionmap_table, "id", collectionmap_id[0]['id'])
        self.delete_record_attr(
            collection_table, parent_id, "collectionmap_id")

    def test_update_remove_attr_from_table(self):
        item_dict = {}
        item_dict['create_date'] = None
        item_dict['modified_date'] = None
        item_dict['circa'] = None
        utc_now = lambda_function.utcformat(datetime.now())
        item_dict['updatedAt'] = utc_now
        result = lambda_function.update_remove_attr_from_table(
            collection_table, item_dict, "073f946b-4d26-4da9-bff0-a0a3606f0015")
        assert result['ResponseMetadata']['HTTPStatusCode'] == 200

    def test_update_item_in_table(self):
        csv_file = 'tests/unit/test_data/collection_metadata.csv'
        df = lambda_function.csv_to_dataframe(csv_file)
        collection_dict = lambda_function.process_csv_metadata(
            df.iloc[0], 'Collection')

        items = lambda_function.query_by_index(
            collection_table, 'Identifier', collection_dict['identifier'])

        utc_now = lambda_function.utcformat(datetime.now())
        lambda_function.update_item_in_table(
            collection_table, collection_dict, items[0]['id'])

        result = self.get_record_by_id(collection_table, "id", items[0]['id'])

        assert result[0]['updatedAt'][:16] == utc_now[:16]

    def test_find_and_update(self):
        attr_dict = {}
        attr_dict['identifier'] = "Ms1990_057_F018_023_Wyckoff_Dr"
        result = lambda_function.find_and_update(
            archive_table, attr_dict, 'Archive', 1)
        assert result == "Duplicated"

        attr_dict['identifier'] = "Ms2020-004_vtn0010065"
        lambda_function.find_and_update(archive_table, attr_dict, 'Archive', 1)
        utc_now = lambda_function.utcformat(datetime.now())
        result = lambda_function.query_by_index(
            archive_table, 'Identifier', "Ms2020-004_vtn0010065")
        assert result[0]['updatedAt'][:16] == utc_now[:16]

        attr_dict['identifier'] = "test_archive_identifier"
        lambda_function.find_and_update(archive_table, attr_dict, 'Archive', 1)

        result = lambda_function.query_by_index(
            archive_table, 'Identifier', "test_archive_identifier")
        assert result[0]['identifier'] == "test_archive_identifier"
        self.delete_table_record(archive_table, "id", result[0]['id'])
        self.delete_table_record(
            mint_table, "short_id", result[0]['custom_key'].split('/')[-1])

    def test_lambda_handler(self):

        #  batch_import_archives
        result = lambda_function.lambda_handler(
            event=test_s3_event_single_archive, context={})
        assert result['statusCode'] == 200

        record = lambda_function.query_by_index(
            archive_table, 'Identifier', "Ms2020-004_com001001")

        assert record[0]['identifier'] == "Ms2020-004_com001001"

        self.delete_table_record(archive_table, "id", record[0]['id'])
        self.delete_table_record(
            mint_table, "short_id", record[0]['custom_key'].split('/')[-1])

        # batch_import_collections
        result = lambda_function.lambda_handler(
            event=test_s3_event_collection, context={})
        assert result['statusCode'] == 200

        record = lambda_function.query_by_index(
            collection_table, 'Identifier', "Ms2007_007_Johnson")

        assert record[0]['identifier'] == "Ms2007_007_Johnson"

        self.delete_table_record(collection_table, "id", record[0]['id'])
        self.delete_table_record(
            collectionmap_table, "id", record[0]['collectionmap_id'])
        self.delete_table_record(
            mint_table, "short_id", record[0]['custom_key'].split('/')[-1])

        # batch_import_archives_with_path
        result = lambda_function.lambda_handler(
            event=test_s3_event_archive_with_path, context={})
        assert result['statusCode'] == 200

        self.verify_delete_SFD_record("sfdst001001")
        self.verify_delete_SFD_record("sfdst001002")
        self.verify_delete_SFD_record("sfdst001003")


if __name__ == "__main__":
    unittest.main()
