import logging
import json
import base64
from tablestore import Condition, RowExistenceExpectation, OTSClientError
import os
from oss2 import Bucket, Auth
from tablestore import OTSClient


def get_table_client():
    """
    Initialize table store client for all CRUD operations
    """
    endpoint = "https://birdtag-table.cn-hongkong.ots.aliyuncs.com"
    access_key_id = os.getenv("TABLE_ACCESS_KEY_ID")
    access_key_secret = os.getenv("TABLE_ACCESS_KEY_SECRET")
    instance_name = "birdtag-table"

    try:
        client = OTSClient(endpoint, access_key_id, access_key_secret, instance_name)
        return client
    except Exception as e:
        raise Exception(f"Client initialization failed: {str(e)}. Check endpoint and AccessKey!")


def delete_media_record(table_name, file_id, timestamp):
    """
    Delete specified record (requires exact primary key match)
    Returns: True if successful, raises Exception if failed
    """
    client = get_table_client()

    primary_key = [
        ('file_id', file_id),
        ('timestamp', timestamp)
    ]

    condition = Condition(RowExistenceExpectation.EXPECT_EXIST)

    try:
        client.delete_row(table_name, primary_key, condition)
        return True
    except Exception as e:
        raise Exception(f"Deletion failed: {str(e)}. Check file_id and timestamp!")


def get_record_by_file_id(table_name, file_id, timestamp=None):
    """
    Query record by file_id (supports exact query or latest record query)
    Args:
        table_name: Name of the target table (fixed as "bird_media_meta")
        file_id: Unique ID of the file (e.g., "img_001")
        timestamp: Optional, exact timestamp for query; None for latest record
    Returns:
        dict: Query result (contains all fields) if success; None if no record
    Raises:
        Exception: If query fails (SDK error or other issues)
    """
    client = get_table_client()

    # Build primary key for query
    primary_key = [('file_id', file_id)]
    if timestamp:
        primary_key.append(('timestamp', timestamp))
    else:
        primary_key.append(('timestamp', None))  # Query latest record if no timestamp

    try:
        # Get response from Table Store (adjust unpack based on SDK return format)
        response = client.get_row(table_name, primary_key)

        # Check if response is a tuple (e.g., (stats, row, others)) or direct Row object
        if isinstance(response, tuple):
            # Case 1: Response is tuple, extract Row object (usually the 2nd element)
            _, row_data, _ = response
        else:
            # Case 2: Response is direct Row object
            row_data = response

        # Check if Row object is valid (no empty record)
        if not row_data or not row_data.primary_key:
            print(f" No data found for file_id: {file_id}")
            return None

        # Organize result into a dictionary (merge primary key and attribute columns)
        record = {}
        # Add primary key fields (format: [(key1, value1), (key2, value2)])
        for key, value in row_data.primary_key:
            record[key] = value
        # Add attribute columns (format: [(key1, value1, timestamp), (key2, value2, timestamp)])
        for key, value, _ in row_data.attribute_columns:
            record[key] = value

        print(f" Data found! file_id: {file_id}")
        return record

    except OTSClientError as e:
        raise Exception(f"Query failed (SDK Error): {e.get_error_message()}")
    except Exception as e:
        raise Exception(f"Query failed: {str(e)}")


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