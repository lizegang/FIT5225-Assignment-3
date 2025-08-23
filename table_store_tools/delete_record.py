from tablestore import Condition, RowExistenceExpectation
from table_store_client import get_table_client

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
    