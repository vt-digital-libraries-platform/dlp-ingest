import boto3, datetime, io, json, logging, os, pathlib
from botocore.response import StreamingBody
from utils.s3_tools import get_matching_s3_keys
from ingest_classes.metadata.generic_metadata import GenericMetadata



class GenericDigitalObject:
    def __init__(self, env, filename, bucket, assets, s3_client, s3_resource):
        self.assets = assets
        self.env = env
        self.filename = filename
        self.bucket = bucket
        self.s3_client = s3_client or boto3.client("s3")
        self.s3_resource = s3_resource or boto3.resource("s3")
        self.lambda_client = boto3.client("lambda")
        self.logger = logging.getLogger()


    def import_digital_objects(self):
        metadataHandler = GenericMetadata(
            self.env, self.filename, self.bucket, self.assets
        )
        metadata = self.local_metadata()
        source_bucket, dest_bucket = self.get_buckets()
        df = metadataHandler.csv_to_dataframe(io.BytesIO(metadata["Body"].read()))

        source_dir = os.path.join(
            self.env["COLLECTION_CATEGORY"], self.env["COLLECTION_IDENTIFIER"]
        )
        # collection assets
        results = self.import_collection_objects(source_bucket, source_dir, dest_bucket)

        # item assets
        results = self.import_item_objects(df, source_bucket, dest_bucket)



    def import_collection_objects(self, source_bucket, source_dir, dest_bucket):
        self.logger.info(f"Beginning collection asset copy")
        for asset in self.assets["collection"]:
            formatted_asset = None
            local_asset = self.assets["collection"][asset] # this is the filename idiot

            # exact, case sensitive search
            formatted_asset = local_asset.replace("<variable>", "")
            success = False
            matches = None
            try:
                matches = get_matching_s3_keys(source_bucket.name, source_dir, formatted_asset)
            except Exception as e:
                self.logger.error(e)

            if matches:
                for key in matches:
                    success = self.format_and_copy(source_bucket, source_dir, key, dest_bucket)
            
            if not success:
                # case insensitive search
                matches = None
                matching_key = None
                asset_path = os.path.join(source_dir, formatted_asset)
                asset_path_no_filename = asset_path.replace(
                    asset_path.split("/")[-1], ""
                )
                try:
                    matches = get_matching_s3_keys(source_bucket.name, asset_path_no_filename)
                except Exception as e:
                    self.logger.error(e)
                if matches:
                    for key in matches:
                        if key.lower() == asset_path.lower():
                            matching_key = key
                            success = self.format_and_copy(source_bucket, source_dir, matching_key, dest_bucket)
                if matching_key is None:
                    self.logger.info("No match found.")



    def import_item_objects(self, df, source_bucket, dest_bucket):
        # item assets
        self.logger.info(f"Beginning item asset copy")
        for idx, row in df.iterrows():
            source_dir, dest_dir = self.get_bucket_paths(row)
            self.logger.info(f"identifier: {row['identifier']}, source_dir: {source_dir}, dest_dir: {dest_dir}")
            for asset in self.assets["item"]:
                success = False
                # if we're supposed to generate thumbnails then just skip the copy here
                if asset == "thumbnail" and self.env["GENERATE_THUMBNAILS"]:
                    break
                
                formatted_asset = None
                local_asset = self.assets["item"][asset]
                success = False
                formatted_asset = local_asset.replace("<item_identifier>", row["identifier"]).replace("<variable>", "")
                matches = None
                matching_key = None

                # exact, case sensitive search
                try:
                    matches = get_matching_s3_keys(source_bucket.name, source_dir, formatted_asset)
                except Exception as e:
                    self.logger.error(e)

                if matches:
                    for key in matches:
                        matching_key = key
                        success = self.format_and_copy(source_bucket, source_dir, key, dest_bucket, dest_dir)
                        
                        if success and self.env["GENERATE_THUMBNAILS"]:
                            self.generate_thumbnail(matching_key, dest_dir)
                else:
                    # case insensitive search
                    asset_path = os.path.join(source_dir, "")
                    matching_key = None
                    matches = None
                    try:
                        matches = get_matching_s3_keys(source_bucket.name, source_dir)
                    except Exception as e:
                        self.logger.error(e)

                    if matches:
                        for key in matches:
                            if key.lower() == asset_path.lower() and not key.endswith("/"):
                                matching_key = key
                                success = self.format_and_copy(source_bucket,source_dir,matching_key,dest_bucket)
                                if success and self.env["GENERATE_THUMBNAILS"]:
                                    self.generate_thumbnail(matching_key, dest_dir)
                                
                                if not success:
                                    self.logger.error(f"No match found for identifier {row['identifier']}")

                
    def generate_thumbnail(self, matching_key, dest_dir):
        # Generate a thumbnail for the object if requested
        try:
            target_key = os.path.join(dest_dir, os.path.basename(matching_key))
            thumbnail_key = target_key.replace(
                f".{self.assets['options']['asset_src']}",
                "_thumbnail.jpg",
            )
            response = self.call_thumbnail_service(
                self.env["AWS_SRC_BUCKET"],
                matching_key,
                self.env["AWS_DEST_BUCKET"],
                thumbnail_key,
            )
            self.logger.info(f"response from thumbnail lambda {response}")
        except Exception as e:
            self.logger.error(e)


    def call_thumbnail_service(self, src_bucket, src_key, dest_bucket, dest_key):
        payload = {
            "src_bucket": src_bucket,
            "src_key": src_key,
            "dest_bucket": dest_bucket,
            "dest_key": dest_key,
        }
        response = self.lambda_client.invoke(
            FunctionName="vtdlp-thumbnail-service",
            InvocationType='Event',
            Payload=bytes(json.dumps(payload), "utf8"),
        )
        return response


    def format_and_copy(
        self, source_bucket, source_dir, key, dest_bucket, dest_dir=None
    ):
        filename = key.split("/")[-1]
        dest_key = os.path.join((dest_dir or source_dir), filename).replace(" ", "_")
        return self.s3_copy(source_bucket, key, dest_bucket, dest_key)


    def s3_copy(self, source_bucket, source_key, dest_bucket, dest_key):
        if self.env["VERBOSE"]:
            self.logger.info(f"Copying: {source_bucket.name}:{source_key}, To: {dest_bucket.name}:{dest_key}")
        if not dest_key.endswith("/"):
            if self.env["DRY_RUN"]:
                self.logger.info(f"DRYRUN: s3 copy: simulated")
                return True
            else:
                try:
                    dest_bucket.copy(
                        {
                            "Bucket": source_bucket.name,
                            "Key": source_key,
                        },
                        dest_key,
                    )
                    return True
                except Exception as e:
                    self.logger.error(e)
                    return False
            
        else:
            return False

    def get_buckets(self):
        return self.s3_resource.Bucket(self.env["AWS_SRC_BUCKET"]), self.s3_resource.Bucket(self.env["AWS_DEST_BUCKET"])

    def get_bucket_paths(self, row):
        src_dir = os.path.join(
            self.env["COLLECTION_CATEGORY"],
            self.env["COLLECTION_IDENTIFIER"],
            row["identifier"]
        )
        dest_dir = os.path.join(
            self.env["COLLECTION_CATEGORY"],
            self.env["COLLECTION_IDENTIFIER"],
            row["identifier"]
        )
        return src_dir, dest_dir

    def local_metadata(self):
        body_encoded = open(self.filename).read().encode()
        stream = StreamingBody(io.BytesIO(body_encoded), len(body_encoded))
        return {"Body": stream}
