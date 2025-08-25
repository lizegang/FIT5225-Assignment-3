# -*- coding: utf-8 -*-
import logging
import json
import base64
from tablestore import Condition, RowExistenceExpectation
import os
from oss2 import Bucket, Auth

from tool import delete_media_record, get_oss_bucket, get_table_client, get_record_by_file_id


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

    oss_bucket = get_oss_bucket()

    if "oss_url" not in req_body:
        return {
            'statusCode': 404,
            'headers': {'Content-Type': 'application/json'},
            'isBase64Encoded': False,
            'body': json.dumps({"error": "oss_url not found"})
        }
    if req_body["oss_url"].startswith("oss://fit5225-a3/"):
        req_body["oss_url"] = req_body["oss_url"][len("oss://fit5225-a3/"):]
    if "thumbnail_url" not in req_body:
        return {
            'statusCode': 404,
            'headers': {'Content-Type': 'application/json'},
            'isBase64Encoded': False,
            'body': json.dumps({"error": "thumbnail_url not found"})
        }
    if req_body["thumbnail_url"].startswith("oss://fit5225-a3/"):
        req_body["thumbnail_url"] = req_body["thumbnail_url"][len("oss://fit5225-a3/"):]

    try:
        delete_media_record(
            table_name=req_body["table_name"],
            file_id=req_body["file_id"],
            timestamp=req_body["timestamp"]
        )
        oss_bucket.delete_object(req_body["oss_url"])
        oss_bucket.delete_object(req_body["thumbnail_url"])
    except Exception as e:
        logger.error("Error deleting objects: %s", e)
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
        'body': json.dumps({"message": "delete success"})
    }
