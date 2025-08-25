from tablestore import OTSClient
import os

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