from tablestore import OTSClient

def get_table_client():
    """
    Initialize table store client for all CRUD operations
    """
    endpoint = "https://birdtag-table.cn-hongkong.ots.aliyuncs.com"
    access_key_id = "your AccessKey ID"
    access_key_secret = "your AccessKey Secret"
    instance_name = "birdtag-table"

    try:
        client = OTSClient(endpoint, access_key_id, access_key_secret, instance_name)
        return client
    except Exception as e:
        raise Exception(f"Client initialization failed: {str(e)}. Check endpoint and AccessKey!")