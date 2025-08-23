from create_record import create_media_record
from get_record import get_record_by_file_id
import time

if __name__ == "__main__":
    table_name = "bird_media_meta"

    # Test data
    test_file_id = "img_test_final_001"
    test_timestamp = int(time.time() * 1000)
    test_oss_url = "oss://test/final.jpg"
    test_file_type = "image"
    test_tags = '{"species":"sparrow","count":2}'
    test_user_id = "user2"
    test_thumbnail_url = "oss://test/final_thumb.jpg"

    # Create record
    try:
        create_result = create_media_record(
            table_name=table_name,
            file_id=test_file_id,
            timestamp=test_timestamp,
            oss_url=test_oss_url,
            file_type=test_file_type,
            tags=test_tags,
            user_id=test_user_id,
            thumbnail_url=test_thumbnail_url
        )
        if create_result:
            print(" Record created successfully!")
    except Exception as e:
        print(f" Creation failed: {e}")
        exit()



    # get record
    try:
        record = get_record_by_file_id(
            table_name=table_name,
            file_id=test_file_id,
            timestamp=test_timestamp
        )
        if record:
            print("\n Query result:")
            for key, value in record.items():
                print(f"{key}: {value}")
    except Exception as e:
        print(f" Query failed: {e}")
    