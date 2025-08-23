from tablestore import Row, Condition, RowExistenceExpectation
from table_store_client import get_table_client

def create_media_record(
    table_name,
    file_id,
    timestamp,
    oss_url,
    file_type,
    tags,
    user_id,
    species=None,  
    count=None,    
    thumbnail_url=None
):
    """
    Create new media metadata record
    Returns: True if successful, raises Exception if failed
    New:

    species: Bird species name (str, e.g., "crow")
    count: Number of this species (int, e.g., 3)
    
    """
    client = get_table_client()

    primary_key = [
        ('file_id', file_id),
        ('timestamp', timestamp)
    ]

    attribute_columns = [
        ('oss_url', oss_url),
        ('file_type', file_type),
        ('tags', tags),
        ('user_id', user_id),
        ('species', species),  
        ('count', count)     
    ]
    if thumbnail_url:
        attribute_columns.append(('thumbnail_url', thumbnail_url))

    row = Row(primary_key, attribute_columns)
    condition = Condition(RowExistenceExpectation.IGNORE)

    try:
        client.put_row(table_name, row, condition)
        return True
    except Exception as e:
        raise Exception(f"Failed to create record: {str(e)}. Check parameters!")

    
