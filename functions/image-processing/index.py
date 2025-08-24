import os
import uuid
import requests
import base64
import re
import time
import json  # 新增：导入json模块用于序列化

from oss2 import Bucket, Auth
from table_store_tools.create_record import create_media_record


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


def call_flask_detect(image_path):
    flask_url = os.getenv("FLASK_DETECT_URL")
    if not flask_url:
        raise ValueError("Flask URL not configured")

    print(f"🚀 Calling Flask API: {flask_url}")
    print(f"📤 Local image path: {image_path}, Size: {os.path.getsize(image_path)} bytes")

    with open(image_path, "rb") as f:
        files = {"image": f}
        response = requests.post(flask_url, files=files, timeout=60)

    print(f"📥 Flask response code: {response.status_code}")
    print(f"📥 Flask response content: {str(response.text)[:500]}...")

    response_data = response.json()
    if response.status_code != 200 or response_data["code"] != 200:
        raise Exception(f"Flask call failed: {response_data['msg']}")

    tags = response_data["data"]["tags"]
    thumbnail_img_base64 = response_data["data"]["thumbnail_image"]
    print(f"✅ Got tags from Flask: {tags}")
    print(f"✅ Thumbnail base64 length: {len(thumbnail_img_base64)} bytes")
    print(f"✅ Thumbnail base64 first 50 chars: {thumbnail_img_base64[:50]}...")

    thumbnail_img_base64 = re.sub(r"[^A-Za-z0-9+/=]", "", thumbnail_img_base64)
    padding = len(thumbnail_img_base64) % 4
    if padding != 0:
        thumbnail_img_base64 += "=" * (4 - padding)
    print(f"✅ Cleaned thumbnail base64 length: {len(thumbnail_img_base64)} bytes")

    try:
        thumbnail_bytes = base64.b64decode(thumbnail_img_base64, validate=True)
        print(f"✅ Thumbnail base64 decoded, Size: {len(thumbnail_bytes)} bytes")
        return tags, thumbnail_bytes
    except base64.binascii.Error as e:
        raise Exception(f"Thumbnail base64 decode failed: {str(e)}, First 50 chars: {thumbnail_img_base64[:50]}")


def handler(event, context):
    try:
        print("=" * 50)
        print("🚀 Start FC image processing function")
        print(f"📥 Received OSS event: {str(event)[:800]}...")

        import json as json_event  # 避免与全局json冲突
        event_data = json_event.loads(event)
        try:
            oss_object = event_data["events"][0]["oss"]["object"]
            print(f"✅ OSS event parsed, File path: {oss_object['key']}, Size: {oss_object['size']} bytes")
        except KeyError:
            raise Exception("OSS event format error: 'events[0].oss.object' not found")

        file_key = oss_object["key"]
        allowed_extensions = [".jpg", ".jpeg", ".png"]
        file_ext = os.path.splitext(file_key)[-1].lower()
        if file_ext not in allowed_extensions:
            return {
                "code": 400,
                "msg": f"Unsupported file type: {file_ext}, Allowed: {allowed_extensions}",
                "data": {}
            }
        print(f"✅ File type validated, Extension: {file_ext}")

        file_id = f"img_{uuid.uuid4().hex[:10]}"
        print(f"✅ Generated unique file ID: {file_id}")

        oss_bucket = get_oss_bucket()
        local_temp_path = f"/tmp/{os.path.basename(file_key)}"
        print(f"📥 Downloading OSS file: {file_key} → Local: {local_temp_path}")

        try:
            oss_bucket.get_object_to_file(file_key, local_temp_path)
            print(f"✅ OSS file downloaded, Local size: {os.path.getsize(local_temp_path)} bytes")
        except Exception as e:
            raise Exception(f"OSS download failed: {str(e)}, File path: {file_key}")

        print("=" * 30)
        tags, thumbnail_bytes = call_flask_detect(local_temp_path)
        print("=" * 30)

        original_filename = os.path.splitext(os.path.basename(file_key))[0]
        thumbnail_key = f"thumbnails/{original_filename}_thumb.jpg"
        thumbnail_temp_path = f"/tmp/thumbnail_{file_id}.jpg"

        with open(thumbnail_temp_path, "wb") as f:
            f.write(thumbnail_bytes)
        print(
            f"✅ Thumbnail written to local: {thumbnail_temp_path}, Size: {os.path.getsize(thumbnail_temp_path)} bytes")

        oss_bucket.put_object_from_file(thumbnail_key, thumbnail_temp_path)
        thumbnail_oss_url = f"oss://{oss_bucket.bucket_name}/{thumbnail_key}"
        print(f"✅ Thumbnail uploaded to OSS, Path: {thumbnail_oss_url}")

        table_name = os.getenv("TABLE_STORE_TABLE_NAME")
        if not table_name:
            raise ValueError("TABLE_STORE_TABLE_NAME not set")
        print(f"📝 Preparing to write to Table Store, Table: {table_name}")

        file_type = "image"
        raw_oss_url = f"oss://{oss_bucket.bucket_name}/{file_key}"
        user_id = "user3"
        # 关键修复：将tags字典转为JSON字符串
        tags_str = json.dumps(tags)
        print(f"📝 转换后的tags字符串: {tags_str}")

        try:
            table_result = create_media_record(
                table_name=table_name,
                file_id=file_id,
                timestamp=int(time.time()),
                oss_url=raw_oss_url,
                file_type=file_type,
                tags=tags_str,  # 传入字符串格式的tags
                user_id=user_id,
                thumbnail_url=thumbnail_oss_url
            )
            print(f"✅ Table Store write done, Result: {table_result}")
        except Exception as e:
            raise Exception(f"Table Store write failed: {str(e)}, Table: {table_name}")

        if os.path.exists(local_temp_path):
            os.remove(local_temp_path)
            print(f"🗑️ Deleted local original file: {local_temp_path}")
        if os.path.exists(thumbnail_temp_path):
            os.remove(thumbnail_temp_path)
            print(f"🗑️ Deleted local thumbnail file: {thumbnail_temp_path}")

        print("=" * 50)
        print("🎉 FC image processing function succeeded!")
        return {
            "code": 200,
            "msg": "Image processing completed",
            "data": {
                "file_id": file_id,
                "raw_oss_url": raw_oss_url,
                "thumbnail_oss_url": thumbnail_oss_url,
                "tags": tags,
                "table_write_result": table_result
            }
        }

    except Exception as e:
        error_msg = f"Processing failed: {str(e)}"
        print(f"❌ {error_msg}")
        print("=" * 50)
        return {
            "code": 500,
            "msg": error_msg,
            "data": {}
        }
