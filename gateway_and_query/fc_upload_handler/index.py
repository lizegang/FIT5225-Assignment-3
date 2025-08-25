# -*- coding: utf-8 -*-
import logging
import json
import base64
import os
from oss2 import Bucket, Auth
from tool import get_oss_bucket
import uuid


def handler(event, context):
    logger = logging.getLogger()
    logger.info("receive event: %s", event)

    try:
        event_json = json.loads(event)
    except:
        return "The request did not come from an HTTP Trigger because the event is not a json string, event: {}".format(
            event)

    if "body" not in event_json:
        return "The request did not come from an HTTP Trigger because the event does not include the 'body' field, event: {}".format(
            event)

    req_body = event_json['body']
    if 'isBase64Encoded' in event_json and event_json['isBase64Encoded']:
        req_body = base64.b64decode(event_json['body']).decode("utf-8")
    req_body = json.loads(req_body)

    required_fields = ["file_name", "file_content_base64"]
    for field in required_fields:
        if field not in req_body:
            return {
                'statusCode': 400,
                'headers': {'Content-Type': 'application/json'},
                'isBase64Encoded': False,
                'body': json.dumps({"error": f"{field} not found"})
            }

    oss_bucket = get_oss_bucket()

    # 保留原文件扩展名
    file_name = req_body["file_name"]
    oss_key = f"raw/{file_name}"  # 用 file_id 保证唯一性

    # OSS 上传
    try:
        file_bytes = base64.b64decode(req_body["file_content_base64"])
        oss_bucket.put_object(oss_key, file_bytes)
    except Exception as e:
        logger.error("Error uploading file to OSS: %s", e)
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'isBase64Encoded': False,
            'body': json.dumps({"error": str(e)})
        }

    return {
        'statusCode': 200,
        'headers': {'Content-Type': 'application/json'},
        'isBase64Encoded': False,
        'body': json.dumps({
            "message": "upload success",
            "oss_url": f"oss://fit5225-a3/{oss_key}"
        })
    }
