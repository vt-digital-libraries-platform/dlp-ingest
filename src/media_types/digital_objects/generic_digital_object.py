import boto3, datetime, io, json, os, pathlib
from botocore.response import StreamingBody
if os.getenv("GUI") is not None and os.getenv("GUI").lower() == "true":
    from dlp_ingest.src.utils.s3_tools import get_matching_s3_keys
    from dlp_ingest.src.media_types.metadata.generic_metadata import GenericMetadata
else:
    from src.utils.s3_tools import get_matching_s3_keys
    from src.media_types.metadata.generic_metadata import GenericMetadata



class GenericDigitalObject:
    def __init__(self, env, filename, bucket, assets, s3_client, s3_resource):
        self.assets = assets
        self.env = env
        self.filename = filename
        self.bucket = bucket
        self.s3_client = s3_client or boto3.client("s3")
        self.s3_resource = s3_resource or boto3.resource("s3")
        self.lambda_client = boto3.client("lambda")

    def import_digital_objects(self):
        metadataHandler = GenericMetadata(
            self.env, self.filename, self.bucket, self.assets
        )
        metadata = self.local_metadata()
        source_bucket, dest_bucket = self.get_buckets()
        df = metadataHandler.csv_to_dataframe(io.BytesIO(metadata["Body"].read()))

        num_successful = 0
        successful_copies = []
        num_failed = 0
        failed_copies = []

        source_dir = os.path.join(
            self.env["COLLECTION_CATEGORY"], self.env["COLLECTION_IDENTIFIER"]
        )
        print("category: " + self.env["COLLECTION_CATEGORY"])
        print("collection identifier: " + self.env["COLLECTION_IDENTIFIER"])
        print("source_dir: " + source_dir)
        print()

        print("----------------")
        print("Collection assets")
        print("----------------")
        # collection assets
        for asset in self.assets["collection"]:
            formatted_asset = None
            print("--------")
            print("asset: " + asset + " = " + str(self.assets["collection"][asset]))
            # collection: list
            if type(self.assets["collection"][asset]) is list:
                for item in self.assets["collection"][asset]:
                    formatted_asset = item.replace("<variable>", "")
                    asset_path = os.path.join(source_dir, formatted_asset)
                    success = False
                    for key in get_matching_s3_keys(
                        source_bucket.name, source_dir, formatted_asset
                    ):
                        print(f"Collection list: {key}")
                        print("Exact Match")
                        success = self.format_and_copy(
                            source_bucket, source_dir, key, dest_bucket
                        )
                    if not success:
                        print("No match found, trying to ignore filename case")
                        asset_path_no_filename = asset_path.replace(
                            asset_path.split("/")[-1], ""
                        )
                        matching_key = None
                        for key in get_matching_s3_keys(
                            source_bucket.name, asset_path_no_filename
                        ):
                            if key.lower() == asset_path.lower():
                                matching_key = key
                                print(f"Collection list: {key}")
                                print(f"Case Insentive Match")
                                success = self.format_and_copy(
                                    source_bucket, source_dir, matching_key, dest_bucket
                                )
                        if matching_key is None:
                            print("No match found still? I got nothing.")

                        num_successful, successful_copies, num_failed, failed_copies = (
                            self.log_copy(
                                key,
                                success,
                                num_successful,
                                successful_copies,
                                num_failed,
                                failed_copies,
                            )
                        )
                        # end collection: list
            # collection: string
            else:
                success = False
                formatted_asset = self.assets["collection"][asset].replace(
                    "<variable>", ""
                )
                asset_path = os.path.join(source_dir, formatted_asset)
                # match on the entire key, case sensitive
                for key in get_matching_s3_keys(
                    source_bucket.name, source_dir, formatted_asset
                ):
                    print(f"Collection string: {key}")
                    print("Exact Match")
                    success = self.format_and_copy(
                        source_bucket, source_dir, key, dest_bucket
                    )
                if not success:
                    print("No match found, trying to ignore filename case")
                    asset_path_no_filename = asset_path.replace(
                        asset_path.split("/")[-1], ""
                    )
                    matching_key = None
                    for key in get_matching_s3_keys(
                        source_bucket.name, asset_path_no_filename
                    ):
                        if key.lower() == asset_path.lower():
                            matching_key = key
                            print(f"Collection string: {key}")
                            print(f"Case Insentive Match")
                            success = self.format_and_copy(
                                source_bucket, source_dir, matching_key, dest_bucket
                            )
                    if matching_key is None:
                        print("No match found still? I got nothing.")

                num_successful, successful_copies, num_failed, failed_copies = (
                    self.log_copy(
                        key,
                        success,
                        num_successful,
                        successful_copies,
                        num_failed,
                        failed_copies,
                    )
                )
        print("----------------")
        print("Item assets")
        print("----------------")

        # item assets
        for idx, row in df.iterrows():
            print("\n========")
            print("identifier: " + row.identifier)
            source_dir, dest_dir = self.get_bucket_paths(row)
            for asset in self.assets["item"]:
                success = False
                print("\n----")
                print("asset: " + asset + " = " + str(self.assets["item"][asset]))
                if asset == "thumbnail" and self.env["GENERATE_THUMBNAILS"]:
                    print("Thumbnail generation is set. Skipping copy.")
                    continue
                formatted_asset = None
                # item: list
                if type(self.assets["item"][asset]) is list:
                    for item in self.assets["item"][asset]:
                        formatted_asset = item.replace(
                            "<item_identifier>", row["identifier"]
                        ).replace("<variable>", "")
                        for key in get_matching_s3_keys(
                            source_bucket.name, source_dir, formatted_asset
                        ):
                            print(f"Item list: {key}")
                            print("Exact Match")
                            success = self.format_and_copy(
                                source_bucket,
                                source_dir,
                                key,
                                dest_bucket,
                                dest_dir,
                            )
                        if not success:
                            print("No match found, trying to ignore filename case")
                            asset_path = os.path.join(source_dir, formatted_asset)
                            matching_key = None
                            for key in get_matching_s3_keys(source_bucket, source_dir):
                                if key.lower() == asset_path.lower():
                                    matching_key = key
                                    print(f"Item list: {key}")
                                    print(f"Case Insentive Match")
                                    success = self.format_and_copy(
                                        source_bucket,
                                        source_dir,
                                        matching_key,
                                        dest_bucket,
                                    )
                            if matching_key is None:
                                print("No match found still? I got nothing.")

                        (
                            num_successful,
                            successful_copies,
                            num_failed,
                            failed_copies,
                        ) = self.log_copy(
                            key,
                            success,
                            num_successful,
                            successful_copies,
                            num_failed,
                            failed_copies,
                        )
                        # end item: list
                        if success:
                            print("GREAT SUCCESS")
                # item: string
                else:
                    success = False
                    formatted_asset = (
                        self.assets["item"][asset]
                        .replace("<item_identifier>", row["identifier"])
                        .replace("<variable>", "")
                    )
                    print(f"Formatted asset: {formatted_asset}")

                    for key in get_matching_s3_keys(
                        source_bucket.name, source_dir, formatted_asset
                    ):
                        print("Exact Match")
                        success = self.format_and_copy(
                            source_bucket, source_dir, key, dest_bucket, dest_dir
                        )
                        print(f"Copy success: {str(success)}")
                    if not success:
                        print("No match found, trying to ignore filename case")
                        asset_path = os.path.join(source_dir, "")
                        matching_key = None
                        for key in get_matching_s3_keys(source_bucket.name, source_dir):
                            if key.lower() == asset_path.lower() and not key.endswith(
                                "/"
                            ):
                                matching_key = key
                                print(f"Case Insentive Match")
                                success = self.format_and_copy(
                                    source_bucket,
                                    source_dir,
                                    matching_key,
                                    dest_bucket,
                                )
                                print(f"Copy success: {str(success)}")
                        if matching_key is None:
                            print("No match found still? I got nothing.")

                    (
                        num_successful,
                        successful_copies,
                        num_failed,
                        failed_copies,
                    ) = self.log_copy(
                        key,
                        success,
                        num_successful,
                        successful_copies,
                        num_failed,
                        failed_copies,
                    )
                    try:
                        if (
                            success
                            and self.env["GENERATE_THUMBNAILS"]
                            and key.endswith(self.assets["options"]["asset_src"])
                        ):
                            target_key = os.path.join(dest_dir, os.path.basename(key))
                            thumbnail_key = target_key.replace(
                                f".{self.assets['options']['asset_src']}",
                                "_thumbnail.jpg",
                            )
                            self.call_thumbnail_service(
                                self.env["AWS_SRC_BUCKET"],
                                key,
                                self.env["AWS_DEST_BUCKET"],
                                thumbnail_key,
                            )
                    except Exception as e:
                        print(e)
        self.log_results(num_successful, successful_copies, num_failed, failed_copies)

    def call_thumbnail_service(self, src_bucket, src_key, dest_bucket, dest_key):
        payload = {
            "src_bucket": src_bucket,
            "src_key": src_key,
            "dest_bucket": dest_bucket,
            "dest_key": dest_key,
        }
        print("Calling thumbnail service with payload:")
        print(payload)
        response = self.lambda_client.invoke(
            FunctionName="vtdlp-thumbnail-service",
            InvocationType='Event',
            Payload=bytes(json.dumps(payload), "utf8"),
        )
        print("Thumbnail service response:")
        print(response)

    def log_results(self, num_successful, successful_copies, num_failed, failed_copies):
        success_msg = f"Successfully copied {num_successful} files.\n"
        print()
        print("----------------")
        print(success_msg)
        failed_msg = f"Failed to copy {num_failed} files.\n"
        print(failed_msg)
        now = datetime.datetime.now()
        output_path = os.path.join(self.env["SCRIPT_ROOT"], "results_files")
        pathlib.Path(output_path).mkdir(parents=True, exist_ok=True)
        out_filename = os.path.join(
            output_path, str(now).replace(" ", "_") + "_ingest_results.txt"
        )
        with open(out_filename, "w") as f:
            f.write(failed_msg)
            for item in failed_copies:
                f.write(f"{item}\n")
            f.write(
                "------------------------------------------------------------------\n"
            )
            f.write(success_msg)
            for item in successful_copies:
                f.write(f"{item}\n")

    def log_copy(
        self, key, success, num_successful, successful_copies, num_failed, failed_copies
    ):
        if success:
            num_successful = num_successful + 1
            successful_copies.append(key)
        else:
            num_failed = num_failed + 1
            failed_copies.append(key)
        return num_successful, successful_copies, num_failed, failed_copies

    def format_and_copy(
        self, source_bucket, source_dir, key, dest_bucket, dest_dir=None
    ):
        filename = key.split("/")[-1]
        dest_key = os.path.join((dest_dir or source_dir), filename).replace(" ", "_")
        return self.s3_copy(source_bucket, key, dest_bucket, dest_key)

    def s3_copy(self, source_bucket, source_key, dest_bucket, dest_key):
        if self.env["VERBOSE"]:
            print("Copying:")
            print(f"{source_bucket.name}:{source_key}")
            print("to:")
            print(f"{dest_bucket.name}:{dest_key}")
        if not dest_key.endswith("/"):
            if self.env["DRY_RUN"]:
                print(f"DRYRUN: s3 copy: simulated")
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
                    print(e)
                    return False
            
        else:
            return False

    def get_buckets(self):
        return self.s3_resource.Bucket(
            self.env["AWS_SRC_BUCKET"]
        ), self.s3_resource.Bucket(self.env["AWS_DEST_BUCKET"])

    def get_bucket_paths(self, row):
        src_dir = os.path.join(
            self.env["COLLECTION_CATEGORY"],
            self.env["COLLECTION_IDENTIFIER"],
            self.env["COLLECTION_SUBDIRECTORY"],
            row["identifier"],
        )
        dest_dir = os.path.join(
            self.env["COLLECTION_CATEGORY"],
            self.env["COLLECTION_IDENTIFIER"],
            row["identifier"],
            self.env["ITEM_SUBDIRECTORY"],
        )
        return src_dir, dest_dir

    def local_metadata(self):
        body_encoded = open(self.filename).read().encode()
        stream = StreamingBody(io.BytesIO(body_encoded), len(body_encoded))
        return {"Body": stream}
