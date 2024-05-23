import datetime, io, os, pathlib, sys
from lib_files.s3_tools import get_matching_s3_keys
from lib_files.media_types.metadata.generic_metadata import GenericMetadata
from botocore.response import StreamingBody


class GenericDigitalObject:
    def __init__(self, env, filename, bucket, assets):
        self.assets = assets
        self.env = env
        self.filename = filename
        self.bucket = bucket

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
            self.env["collection_category"], self.env["collection_identifier"]
        )
        # collection assets
        for asset in self.assets["collection"]:
            formatted_asset = None
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
                            source_bucket, asset_path_no_filename
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
            # end collection assets

            # item assets
            for idx, row in df.iterrows():
                source_dir, dest_dir = self.get_bucket_paths(row)

                for asset in self.assets["item"]:
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
                                    print(
                                        "No match found, trying to ignore filename case"
                                    )
                                    asset_path = os.path.join(
                                        source_dir, formatted_asset
                                    )
                                    matching_key = None
                                    for key in get_matching_s3_keys(
                                        source_bucket, source_dir
                                    ):
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

                    # item: string
                    else:
                        formatted_asset = (
                            self.assets["item"][asset]
                            .replace("<item_identifier>", row["identifier"])
                            .replace("<variable>", "")
                        )
                        for key in get_matching_s3_keys(
                            source_bucket.name, source_dir, formatted_asset
                        ):
                            print(f"Item string: {key}")
                            print("Exact Match")
                            success = self.format_and_copy(
                                source_bucket, source_dir, key, dest_bucket, dest_dir
                            )
                            if not success:
                                print("No match found, trying to ignore filename case")
                                asset_path = os.path.join(source_dir, formatted_asset)
                                matching_key = None
                                for key in get_matching_s3_keys(
                                    source_bucket, source_dir
                                ):
                                    if key.lower() == asset_path.lower():
                                        matching_key = key
                                        print(f"Item string: {key}")
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
        self.log_results(num_successful, successful_copies, num_failed, failed_copies)

    def log_results(self, num_successful, successful_copies, num_failed, failed_copies):
        success_msg = f"Successfully copied {num_successful} files.\n"
        print(success_msg)
        failed_msg = f"Failed to copy {num_failed} files.\n"
        print(failed_msg)
        now = datetime.datetime.now()
        output_path = os.path.join(self.env["script_root"], "results_files")
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
        if self.env["verbose"]:
            print("Copying")
            print(f"{source_bucket.name}:{source_key}")
            print("to")
            print(f"{dest_bucket.name}:{dest_key}")
            print()
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

    def get_buckets(self):
        return self.s3_resource.Bucket(
            self.env["aws_src_bucket"]
        ), self.s3_resource.Bucket(self.env["aws_dest_bucket"])

    def get_bucket_paths(self, row):
        src_dir = os.path.join(
            self.env["collection_category"],
            self.env["collection_identifier"],
            self.env["collection_subdirectory"],
            row["identifier"],
        )
        dest_dir = os.path.join(
            self.env["collection_category"],
            self.env["collection_identifier"],
            row["identifier"],
            "3d",
        )
        return src_dir, dest_dir

    def local_metadata(self):
        body_encoded = open(self.filename).read().encode()
        stream = StreamingBody(io.BytesIO(body_encoded), len(body_encoded))
        return {"Body": stream}
