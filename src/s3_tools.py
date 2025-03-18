#!/usr/bin/python3
import boto3

def get_matching_s3_keys(bucket, prefix="", suffix="", client=None):
    client = client or boto3.client('s3')
    prefix = str(prefix)
    suffix = str(suffix)
    kwargs = {"Bucket": bucket}
    kwargs["Prefix"] = prefix
    matching_keys = []

    while True:
        resp = client.list_objects_v2(**kwargs)
        try:
            contents = resp["Contents"]
        except KeyError:
            break
        for obj in contents:
            key = obj["Key"]
            if key.startswith(prefix) and key.endswith(suffix):
                matching_keys.append(key)
        try:
            kwargs["ContinuationToken"] = resp["NextContinuationToken"]
        except KeyError:
            break

    return matching_keys