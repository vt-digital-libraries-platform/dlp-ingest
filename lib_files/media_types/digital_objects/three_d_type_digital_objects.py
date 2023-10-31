import boto3
import io
import datetime
from lib_files.media_types.digital_objects.generic_type_digital_objects import GenericTypeDigitalObjects
from lib_files.media_types.metadata.generic_type_metadata import GenericTypeMetadata

class ThreeDTypeDigitalObjects(GenericTypeDigitalObjects):
  def __init__(self, env, headers_keys, metadata_filename, metadata):
    super().__init__(env, headers_keys, metadata_filename, metadata)


  def import_digital_objects(self):
    prod = boto3.session.Session(profile_name='prod')
    s3 = prod.resource('s3')
    src_bucket_name = self.env["src_bucket"]
    source_bucket = s3.Bucket(src_bucket_name)
    target_bucket_name = self.env["target_bucket"]
    target_bucket = s3.Bucket(target_bucket_name)
    metadataHandler = GenericTypeMetadata(self.env, self.headers_keys, self.metadata_filename, self.metadata)

    df = metadataHandler.csv_to_dataframe(io.BytesIO(self.metadata['Body'].read()))
    source_dir = None
    target_dir = None
    try:
      source_dir = f"{self.env['collection_category']}/3d/{self.env['src_prefix']}"
      target_dir = source_dir.lower()
    except Exception as e:
      print(f"An error occurred: {str(e)}")

    successful_copies = []
    num_successful = 0
    failed_copies = []
    num_failed = 0
    for idx, row in df.iterrows():
      source_key = f"{source_dir}/{row['identifier']}"
      print(f"source_key: {source_key}")
      for item_obj in source_bucket.objects.all():
        if source_key in item_obj.key:
          target_key = item_obj.key.replace(" ", "_")
          if ".DS_Store" not in target_key:
            copy_source = {
              'Bucket': src_bucket_name,
              'Key': item_obj.key
            }
            try:
              # target_bucket.copy(copy_source, target_key)
              num_successful = num_successful + 1
              print(f"Successfully copied {item_obj.key} to {target_key}")
              successful_copies.append(item_obj.key)
            except Exception as e:
              print(f"An error occurred: {str(e)}")
              print(f"Failed to copy {src_bucket_name}:{item_obj.key} to {target_bucket_name}:{target_key}")
              num_failed = num_failed + 1
              failed_copies.append(item_obj.key)
            print("----------------------------------------------------------------------")



    print("==========================================================================")
    success_msg = f"Successfully copied {num_successful} files.\n"
    print(success_msg)
    failed_msg = f"Failed to copy {num_failed} files.\n"
    print(failed_msg)
    now = datetime.datetime.now()
    out_filename = f"output/{str(now).replace(' ','_')}_ingest_results.txt"
    with open(out_filename, "w") as f:
      f.write(failed_msg)
      for item in failed_copies:
        f.write(f"{item}\n")
      f.write("------------------------------------------------------------------\n")
      f.write(success_msg)
      for item in successful_copies:
        f.write(f"{item}\n")
      
      