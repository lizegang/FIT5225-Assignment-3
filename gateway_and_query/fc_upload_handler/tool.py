import logging
import json
import base64
from tablestore import Condition, RowExistenceExpectation, OTSClientError
import os
from oss2 import Bucket, Auth
from tablestore import OTSClient

def get_oss_bucket():
    access_key_id = os.getenv("ALIYUN_ACCESS_KEY_ID")
    access_key_secret = os.getenv("ALIYUN_ACCESS_KEY_SECRET")
    endpoint = os.getenv("OSS_ENDPOINT")
    bucket_name = os.getenv("OSS_BUCKET_NAME")

    if not all([access_key_id, access_key_secret, endpoint, bucket_name]):
        raise ValueError("OSS config incomplete")

    auth = Auth(access_key_id, access_key_secret)
    bucket = Bucket(auth, endpoint, bucket_name)
    print(f"✅ OSS client init done, Bucket: {bucket_name}, Endpoint: {endpoint}")
    return bucket