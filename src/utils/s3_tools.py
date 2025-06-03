#!/usr/bin/python3
import boto3

def get_matching_s3_keys(bucket, prefix="", suffix=""):
    s3 = boto3.client("s3")
    kwargs = {"Bucket": bucket, "Prefix": prefix}
    matches = []
    while True:
        resp = s3.list_objects_v2(**kwargs)
        try:
            contents = resp["Contents"]
        except KeyError:
            return
        for obj in contents:
            key = obj["Key"]
            if key.startswith(prefix) and key.endswith(suffix):
                matches.append(key)
        try:
            kwargs["ContinuationToken"] = resp["NextContinuationToken"]
        except KeyError:
            break
    return matches