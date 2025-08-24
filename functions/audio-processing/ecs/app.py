from flask import Flask, request, jsonify
import os
import logging
from dotenv import load_dotenv
import requests
import json
from audio_processor import AudioProcessor

# 加载环境变量
load_dotenv()

app = Flask(__name__)

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 初始化音频处理器
audio_processor = AudioProcessor()

@app.route('/health', methods=['GET'])
def health_check():
    """健康检查端点"""
    return jsonify({
        'status': 'healthy',
        'service': 'audio-processing',
        'version': '1.0.0'
    }), 200

@app.route('/info', methods=['GET'])
def service_info():
    """服务信息端点"""
    return jsonify({
        'service': 'audio-processing',
        'version': '1.0.0',
        'description': 'ECS Audio Processing Service with BirdNET-Analyzer',
        'endpoints': {
            '/health': 'Health check',
            '/info': 'Service information',
            '/process': 'Process audio file from URL',
            '/process_audio': 'Process audio file from OSS (FC trigger)'
        },
        'dependencies': {
            'BirdNET-Analyzer': 'Integrated',
            'OSS': 'Connected',
            'TableStore': 'Connected'
        }
    }), 200

@app.route('/process', methods=['POST'])
def process_audio_from_url():
    """从URL处理音频文件"""
    try:
        data = request.get_json()
        
        if not data or 'file_url' not in data:
            return jsonify({
                'error': 'Missing required parameter: file_url'
            }), 400
        
        file_url = data['file_url']
        metadata = data.get('metadata', {})
        
        logger.info(f"Processing audio from URL: {file_url}")
        
        # 调用音频处理逻辑
        result = audio_processor.process_audio_from_url(file_url, metadata)
        
        return jsonify({
            'status': 'success',
            'message': 'Audio processed successfully',
            'processing_time': result.get('processing_time', 0),
            'bird_detections': result.get('detections', []),
            'storage_info': {
                'oss_url': result.get('oss_url'),
                'table_record_id': result.get('record_id')
            }
        }), 200
        
    except Exception as e:
        logger.error(f"Error processing audio from URL: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@app.route('/process_audio', methods=['POST'])
def process_audio():
    """处理音频文件 (FC触发器使用)"""
    try:
        data = request.get_json()
        
        if not data or 'bucket_name' not in data or 'object_key' not in data:
            return jsonify({
                'error': 'Missing required parameters: bucket_name, object_key'
            }), 400
        
        bucket_name = data['bucket_name']
        object_key = data['object_key']
        
        logger.info(f"Processing audio: {bucket_name}/{object_key}")
        
        # 调用音频处理逻辑
        result = audio_processor.process_audio_file(bucket_name, object_key)
        
        return jsonify({
            'status': 'success',
            'message': 'Audio processed successfully',
            'result': result
        }), 200
        
    except Exception as e:
        logger.error(f"Error processing audio: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

if __name__ == '__main__':
    port = int(os.getenv('PORT', 5000))
    debug = os.getenv('DEBUG', 'False').lower() == 'true'
    
    logger.info(f"Starting audio processing service on port {port}")
    app.run(host='0.0.0.0', port=port, debug=debug)
