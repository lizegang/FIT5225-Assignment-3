# -*- coding: utf-8 -*-
import json
import requests
import logging
import os
from urllib.parse import unquote

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 环境变量配置
ECS_SERVICE_URL = os.environ.get('ECS_SERVICE_URL', 'http://47.76.60.159:5000')
ACCESS_KEY_ID = os.environ.get('ACCESS_KEY_ID')
ACCESS_KEY_SECRET = os.environ.get('ACCESS_KEY_SECRET')
OSS_BUCKET_NAME = os.environ.get('OSS_BUCKET_NAME', 'fit5225-a3')
OSS_ENDPOINT = os.environ.get('OSS_ENDPOINT', 'https://oss-cn-hongkong.aliyuncs.com')
OSS_AUDIO_PREFIX = os.environ.get('OSS_AUDIO_PREFIX', 'raw/')
OSS_THUMBNAIL_PREFIX = os.environ.get('OSS_THUMBNAIL_PREFIX', 'results/')
TABLE_STORE_INSTANCE = os.environ.get('TABLE_STORE_INSTANCE', 'birdtag-table')
TABLE_STORE_ENDPOINT = os.environ.get('TABLE_STORE_ENDPOINT', 'https://birdtag-table.cn-hongkong.ots.aliyuncs.com')
Table_name = os.environ.get('Table_name', 'bird_media_meta')

def is_audio_file(object_key):
    """
    检查文件是否为音频文件
    """
    audio_extensions = ['.wav', '.mp3', '.flac', '.aac', '.ogg', '.m4a']
    return any(object_key.lower().endswith(ext) for ext in audio_extensions)

def safe_decode_response_text(response_text):
    """
    安全地将response.text转换为字符串，处理bytes类型
    """
    if isinstance(response_text, bytes):
        try:
            return response_text.decode('utf-8')
        except UnicodeDecodeError:
            return response_text.decode('utf-8', errors='replace')
    return str(response_text)

def handler(event, context):
    """
    FC中转函数 - 将OSS事件转发给ECS音频处理服务
    """
    try:
        # 处理bytes类型的事件数据
        if isinstance(event, bytes):
            try:
                # 将bytes解码为字符串，然后解析为JSON
                event_str = event.decode('utf-8')
                event = json.loads(event_str)
                logger.info(f"Successfully decoded bytes event to JSON")
            except (UnicodeDecodeError, json.JSONDecodeError) as e:
                logger.error(f"Failed to decode bytes event: {str(e)}")
                return {
                    'statusCode': 400,
                    'body': json.dumps({
                        'error': 'Invalid event format',
                        'message': f'Failed to decode bytes event: {str(e)}'
                    }, ensure_ascii=False)
                }
        
        # 安全记录事件信息，避免bytes序列化问题
        try:
            logger.info(f"Received event: {json.dumps(event, ensure_ascii=False)}")
        except (TypeError, ValueError) as e:
            logger.info(f"Received event (contains non-serializable data): {type(event)} with keys: {list(event.keys()) if isinstance(event, dict) else 'not a dict'}")
            logger.debug(f"Event serialization error: {str(e)}")
        
        # 解析OSS事件
        if 'events' in event:
            results = []
            for oss_event in event['events']:
                bucket_name = oss_event['oss']['bucket']['name']
                object_key = unquote(oss_event['oss']['object']['key'])  # URL解码
                
                logger.info(f"Processing file: {bucket_name}/{object_key}")
                
                # 检查文件类型是否为音频
                if not is_audio_file(object_key):
                    logger.info(f"Skipping non-audio file: {object_key}")
                    continue
                
                # 调用ECS音频处理服务
                try:
                    # 构建OSS文件URL
                    file_url = f"https://{bucket_name}.oss-cn-hongkong.aliyuncs.com/{object_key}"
                    
                    response = requests.post(
                        f"{ECS_SERVICE_URL}/process",
                        json={
                            "file_url": file_url
                        },
                        headers={'Content-Type': 'application/json'},
                        timeout=300
                    )
                    
                    if response.status_code == 200:
                        result = response.json()
                        logger.info(f"Successfully processed {object_key}: {result}")
                        results.append({
                            'file': object_key,
                            'status': 'success',
                            'result': result
                        })
                    else:
                        error_msg = f"ECS service error: {response.status_code}"
                        try:
                            # 安全地处理response.text，避免JSON序列化错误
                            error_detail = safe_decode_response_text(response.text)
                            if error_detail:
                                error_msg += f" - {error_detail}"
                                # 在日志中记录详细错误，但不包含在JSON响应中
                                logger.error(f"ECS service error details: {error_detail}")
                        except Exception as e:
                            logger.error(f"Error processing response text: {str(e)}")
                        
                        results.append({
                            'file': object_key,
                            'status': 'error',
                            'error': f'ECS service error: {response.status_code}'
                        })
                        
                except requests.exceptions.Timeout:
                    logger.error(f"Timeout when processing {object_key}")
                    results.append({
                        'file': object_key,
                        'status': 'error',
                        'error': 'ECS service timeout'
                    })
                except requests.exceptions.RequestException as e:
                    logger.error(f"Request error when processing {object_key}: {str(e)}")
                    results.append({
                        'file': object_key,
                        'status': 'error',
                        'error': f'Request error: {str(e)}'
                    })
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Audio processing completed',
                    'results': results
                }, ensure_ascii=False)
            }
        
        # 处理测试事件格式
        elif 'bucket_name' in event and 'object_key' in event:
            bucket_name = event['bucket_name']
            object_key = event['object_key']
            
            logger.info(f"Processing test file: {bucket_name}/{object_key}")
            
            if not is_audio_file(object_key):
                return {
                    'statusCode': 400,
                    'body': json.dumps({'error': 'Not an audio file'}, ensure_ascii=False)
                }
            
            try:
                # 构建OSS文件URL
                file_url = f"https://{bucket_name}.oss-cn-hongkong.aliyuncs.com/{object_key}"
                
                response = requests.post(
                    f"{ECS_SERVICE_URL}/process",
                    json={
                        "file_url": file_url
                    },
                    headers={'Content-Type': 'application/json'},
                    timeout=300
                )
                
                if response.status_code == 200:
                    result = response.json()
                    logger.info(f"Successfully processed {object_key}: {result}")
                    return {
                        'statusCode': 200,
                        'body': json.dumps({
                            'message': 'Audio processed successfully',
                            'result': result
                        }, ensure_ascii=False)
                    }
                else:
                    # 安全地处理错误响应，避免JSON序列化问题
                    error_detail = safe_decode_response_text(response.text)
                    logger.error(f"ECS service error: {response.status_code} - {error_detail}")
                    return {
                        'statusCode': response.status_code,
                        'body': json.dumps({
                            'error': f'ECS service error: {response.status_code}',
                            'details': 'Check function logs for more information'
                        }, ensure_ascii=False)
                    }
                    
            except requests.exceptions.Timeout:
                logger.error(f"Timeout when processing {object_key}")
                return {
                    'statusCode': 408,
                    'body': json.dumps({
                        'error': 'ECS service timeout',
                        'message': 'Request to ECS service timed out'
                    }, ensure_ascii=False)
                }
            except requests.exceptions.RequestException as e:
                logger.error(f"Request error when processing {object_key}: {str(e)}")
                return {
                    'statusCode': 500,
                    'body': json.dumps({
                        'error': 'Request error',
                        'message': f'Failed to connect to ECS service: {str(e)}'
                    }, ensure_ascii=False)
                }
        
        # 处理Records格式（标准OSS触发器格式）
        elif 'Records' in event:
            results = []
            for record in event['Records']:
                if 'oss' in record:
                    bucket_name = record['oss']['bucket']['name']
                    object_key = unquote(record['oss']['object']['key'])
                    
                    logger.info(f"Processing OSS record: {bucket_name}/{object_key}")
                    
                    # 检查文件类型是否为音频
                    if not is_audio_file(object_key):
                        logger.info(f"Skipping non-audio file: {object_key}")
                        continue
                    
                    # 调用ECS音频处理服务
                    try:
                        # 构建OSS文件URL
                        file_url = f"https://{bucket_name}.oss-cn-hongkong.aliyuncs.com/{object_key}"
                        
                        response = requests.post(
                            f"{ECS_SERVICE_URL}/process",
                            json={
                                "file_url": file_url
                            },
                            headers={'Content-Type': 'application/json'},
                            timeout=300
                        )
                        
                        if response.status_code == 200:
                            result = response.json()
                            logger.info(f"Successfully processed {object_key}: {result}")
                            results.append({
                                'file': object_key,
                                'status': 'success',
                                'result': result
                            })
                        else:
                            error_detail = safe_decode_response_text(response.text)
                            logger.error(f"ECS service error: {response.status_code} - {error_detail}")
                            results.append({
                                'file': object_key,
                                'status': 'error',
                                'error': f'ECS service error: {response.status_code}'
                            })
                            
                    except requests.exceptions.Timeout:
                        logger.error(f"Timeout when processing {object_key}")
                        results.append({
                            'file': object_key,
                            'status': 'error',
                            'error': 'ECS service timeout'
                        })
                    except requests.exceptions.RequestException as e:
                        logger.error(f"Request error when processing {object_key}: {str(e)}")
                        results.append({
                            'file': object_key,
                            'status': 'error',
                            'error': f'Request error: {str(e)}'
                        })
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Audio processing completed',
                    'results': results
                }, ensure_ascii=False)
            }
        
        else:
            logger.error(f"Unsupported event format: {event}")
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': 'Unsupported event format',
                    'message': 'Event must contain either events, Records, or bucket_name/object_key fields'
                }, ensure_ascii=False)
            }
            
    except Exception as e:
        logger.error(f"Unexpected error in handler: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Internal server error',
                'message': f'Unexpected error: {str(e)}'
            }, ensure_ascii=False)
        }

# 本地测试
if __name__ == '__main__':
    # 测试事件
    test_event = {
        "bucket_name": "fit5225-a3",
        "object_key": "raw/test_bird_audio.wav"
    }
    
    class MockContext:
        def __init__(self):
            self.request_id = "test-request-id"
    
    context = MockContext()
    result = handler(test_event, context)
    print(json.dumps(result, indent=2, ensure_ascii=False))