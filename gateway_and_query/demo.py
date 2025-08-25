import requests
import json


def query_bird_data(url, appcode, body):
    # 请求 URL


    # 请求头
    headers = {
        "Authorization": "APPCODE "+appcode,
        "gateway_channel": "http",
        "X-Ca-Key": "204929743",
        "X-Ca-Request-Mode": "DEBUG",
        "X-Ca-Stage": "RELEASE",
        "Host": "www.fit5032-a3.online",
        "Content-Type": "application/json",
    }

    try:
        response = requests.post(url, headers=headers, json=body)
        response.raise_for_status()  # 如果状态码不是200，会抛出异常
    except requests.RequestException as e:
        print("请求出错:", e)
        return None

    try:
        data = response.json()
        return data
    except json.JSONDecodeError:
        print("返回不合法:", response.text)
        return None


# 调用示例
if __name__ == "__main__":
    body = {
        "species": ["sparrow", "crow"],
        "count_min": 1,
        "count_max": 3,
        "minimum_should_match": 1
    }
    url_analysis = "http://www.fit5032-a3.online/analysis"
    appcode_analysis = "38f6729313934179b7eb6a46476bd431"
    result = query_bird_data(url=url_analysis, appcode=appcode_analysis, body=body)
    print("result:", url_analysis)
    print(result)
    url_query = "http://www.fit5032-a3.online/query"
    appcode_query = "648e7efb47e747d1be2de5039150c834"
    result = query_bird_data(url=url_query, appcode=appcode_query, body=body)
    print("result:", url_query)
    print(result)
    body = {
        "table_name": "bird_media_meta",
        "file_id": "img_5f95dee5b3",
        "timestamp": 1756094505,
        "oss_url": "oss://fit5225-a3/raw/crows_1.jpg",
        "thumbnail_url": "oss://fit5225-a3/thumbnails/crows_1_thumb.jpg",
    }
    url_delete = "http://www.fit5032-a3.online/delete"
    appcode_delete = "e2e0791324e5409693308a4b7aedf1f4"
    result = query_bird_data(url=url_delete, appcode=appcode_delete, body=body)
    print("result:", url_delete)
    print(result)
