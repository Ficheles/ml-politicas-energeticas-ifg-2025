#!/usr/bin/env python3
from pathlib import Path
import boto3

def list_s3_objects(bucket: str, prefix: str = ""):
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)

    total_size = 0
    total_count = 0

    for page in page_iterator:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            size = obj["Size"]
            lm = obj["LastModified"]
            print(f"{key}\t{size} bytes\t{lm}")
            total_count += 1
            total_size += size

    print(f"Total objects: {total_count}, total bytes: {total_size}")
    return total_count, total_size

if __name__ == "__main__":
    list_s3_objects("ml-politicas-energeticas", prefix="inmet/2021/")