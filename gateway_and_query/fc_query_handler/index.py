# -*- coding: utf-8 -*-
import logging
import json
import base64
from multi_tag_query import multi_condition_query
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
    req_body = json.loads(req_body) # dict
    if "minimum_should_match" in req_body and req_body["minimum_should_match"] > 1:
        return {
            'statusCode': 400,
            'headers': {'Content-Type': 'application/json'},
            'isBase64Encoded': False,
            'body': json.dumps({"error": "minimum_should_match must be less than or equal to 1"}, ensure_ascii=False, indent=2)
        }

    search_response = multi_condition_query(
        species=req_body.get("species", []),          # 默认空列表
        species_not=req_body.get("species_not", []), # 默认空列表
        count_min=int(req_body.get("count_min", 1)), # 默认 1
        count_max=int(req_body.get("count_max", 2e9)), # 默认 2
        minimum_should_match=int(req_body.get("minimum_should_match", 1))
    )
    output = json.dumps(search_response, ensure_ascii=False, indent=2) # class json str; search_response.rows class list
    #output = json.loads(output)
    return {
        'statusCode': 200,
        'headers': {'Content-Type': 'application/json'},
        'isBase64Encoded': False,
        'body': output
    }