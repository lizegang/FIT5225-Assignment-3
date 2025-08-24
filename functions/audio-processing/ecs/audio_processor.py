import os
import logging
import tempfile
import time
from datetime import datetime
import oss2
from tablestore import *
import librosa
import librosa.display
import numpy as np
from PIL import Image
import matplotlib
matplotlib.use('Agg')  # 设置无头后端
import matplotlib.pyplot as plt
import io
import sys
import json

# 添加BirdNET-Analyzer路径 - 适配ECS部署结构
# 在ECS服务器上，audio-processing目录与ecs目录在同一级别
birdnet_path = os.path.join(os.path.dirname(__file__), 'audio-processing')
if not os.path.exists(birdnet_path):
    # 如果在开发环境，使用原路径
    birdnet_path = os.path.join(os.path.dirname(__file__), '..', 'functions', 'audio-processing')
sys.path.append(birdnet_path)
try:
    import birdnet_analyzer
    from birdnet_analyzer.analyze.core import analyze
    from birdnet_analyzer import config as cfg
except ImportError as e:
    logging.warning(f"BirdNET-Analyzer not available: {e}. Will use basic audio processing.")

class AudioProcessor:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # OSS配置 - 支持多种环境变量名称格式
        self.access_key_id = os.getenv('OSS_ACCESS_KEY_ID') or os.getenv('ALIYUN_ACCESS_KEY_ID')
        self.access_key_secret = os.getenv('OSS_ACCESS_KEY_SECRET') or os.getenv('ALIYUN_ACCESS_KEY_SECRET')
        self.bucket_name = os.getenv('OSS_BUCKET_NAME')
        self.endpoint = os.getenv('OSS_ENDPOINT')
        self.audio_prefix = os.getenv('OSS_AUDIO_PREFIX', 'raw/')
        self.thumbnail_prefix = os.getenv('OSS_THUMBNAIL_PREFIX', 'results/')
        
        # 表格存储配置 - 支持多种环境变量名称格式
        self.tablestore_endpoint = os.getenv('OTS_ENDPOINT') or os.getenv('TABLE_STORE_ENDPOINT')
        self.tablestore_instance = os.getenv('OTS_INSTANCE_NAME') or os.getenv('TABLE_STORE_INSTANCE')
        self.table_name = os.getenv('OTS_TABLE_NAME') or os.getenv('TABLE_NAME')
        
        # 初始化OSS客户端
        auth = oss2.Auth(self.access_key_id, self.access_key_secret)
        self.bucket = oss2.Bucket(auth, self.endpoint, self.bucket_name)
        
        # 初始化表格存储客户端
        self.ots_client = OTSClient(
            self.tablestore_endpoint,
            self.access_key_id,
            self.access_key_secret,
            self.tablestore_instance
        )
    
    def process_audio_from_url(self, file_url, metadata=None):
        """从URL下载并处理音频文件"""
        import requests
        import urllib.parse
        
        start_time = time.time()
        
        try:
            # 1. 解析OSS URL获取对象键
            self.logger.info(f"Processing audio from OSS URL: {file_url}")
            parsed_url = urllib.parse.urlparse(file_url)
            
            # 从URL路径中提取对象键（去掉开头的/）
            object_key = parsed_url.path.lstrip('/')
            self.logger.info(f"Extracted object key: {object_key}")
            
            # 2. 从OSS下载音频文件到临时文件
            with tempfile.NamedTemporaryFile(suffix='.wav', delete=False) as temp_file:
                self.bucket.get_object_to_file(object_key, temp_file.name)
                audio_path = temp_file.name
            
            # 3. 对象键已从URL中提取，无需重新上传
            # 文件已经在OSS中，直接使用现有的object_key
            
            # 4. 加载和分析音频
            y, sr = librosa.load(audio_path)
            duration = librosa.get_duration(y=y, sr=sr)
            
            # 5. 使用BirdNET-Analyzer进行鸟类识别
            bird_detections = self.analyze_bird_audio(audio_path)
            
            # 6. 生成频谱图
            thumbnail_path = self.generate_spectrogram(y, sr, object_key)
            
            # 7. 上传缩略图到OSS
            thumbnail_key = self.upload_thumbnail(thumbnail_path, object_key)
            
            # 8. 格式化鸟类标签
            formatted_tags = self.format_bird_tags(bird_detections)
            
            # 9. 从object_key提取文件名
            filename = os.path.basename(object_key)
            
            # 10. 保存元数据到表格存储
            file_metadata = {
                'file_name': filename,
                'file_path': object_key,
                'duration': duration,
                'sample_rate': sr,
                'thumbnail_path': thumbnail_key,
                'bird_detections': bird_detections,
                'tags': formatted_tags,
                'processed_time': datetime.now().isoformat(),
                'status': 'processed',
                'source_url': file_url,
                'metadata': metadata or {}
            }
            
            record_id = self.save_metadata(file_metadata)
            
            # 11. 清理临时文件
            os.unlink(audio_path)
            os.unlink(thumbnail_path)
            
            processing_time = time.time() - start_time
            
            return {
                'file_name': filename,
                'duration': duration,
                'detections': bird_detections,
                'tags': formatted_tags,
                'oss_url': f"https://{self.bucket_name}.{self.endpoint.replace('https://', '')}/{object_key}",
                'thumbnail_url': f"https://{self.bucket_name}.{self.endpoint.replace('https://', '')}/{thumbnail_key}",
                'processed_time': file_metadata['processed_time'],
                'processing_time': processing_time,
                'record_id': record_id
            }
              
        except Exception as e:
            self.logger.error(f"Error processing audio from URL {file_url}: {str(e)}")
            raise

    def process_audio_file(self, bucket_name, object_key):
        """处理音频文件的主要逻辑"""
        try:
            # 1. 从OSS下载音频文件
            with tempfile.NamedTemporaryFile(suffix='.wav', delete=False) as temp_file:
                self.bucket.get_object_to_file(object_key, temp_file.name)
                audio_path = temp_file.name
            
            # 2. 加载和分析音频
            y, sr = librosa.load(audio_path)
            duration = librosa.get_duration(y=y, sr=sr)
            
            # 3. 使用BirdNET-Analyzer进行鸟类识别
            bird_detections = self.analyze_bird_audio(audio_path)
            
            # 4. 生成频谱图（可选，用于可视化）
            thumbnail_path = self.generate_spectrogram(y, sr, object_key)
            
            # 5. 上传缩略图到OSS
            thumbnail_key = self.upload_thumbnail(thumbnail_path, object_key)
            
            # 6. 格式化鸟类标签
            formatted_tags = self.format_bird_tags(bird_detections)
            
            # 7. 保存元数据到表格存储
            metadata = {
                'file_name': os.path.basename(object_key),
                'file_path': object_key,
                'duration': duration,
                'sample_rate': sr,
                'thumbnail_path': thumbnail_key,
                'bird_detections': bird_detections,
                'tags': formatted_tags,
                'processed_time': datetime.now().isoformat(),
                'status': 'processed'
            }
            
            self.save_metadata(metadata)
            
            # 8. 清理临时文件
            os.unlink(audio_path)
            os.unlink(thumbnail_path)
             
            return {
                'file_name': metadata['file_name'],
                'duration': duration,
                'bird_detections': bird_detections,
                'tags': formatted_tags,
                'thumbnail_url': f"https://{self.bucket_name}.{self.endpoint.replace('https://', '')}/{thumbnail_key}",
                'processed_time': metadata['processed_time']
            }
              
        except Exception as e:
            self.logger.error(f"Error processing audio file {object_key}: {str(e)}")
            raise
    
    def analyze_bird_audio(self, audio_path):
        """使用BirdNET-Analyzer分析音频中的鸟类"""
        try:
            # 检查BirdNET是否可用
            if 'birdnet_analyzer' not in sys.modules:
                self.logger.warning("BirdNET-Analyzer not available, returning empty detections")
                return []
            
            # 创建临时输出目录
            output_dir = tempfile.mkdtemp()
            # BirdNET的analyze函数期望output参数是目录路径
            output_path = output_dir
            
            # 使用BirdNET-Analyzer分析音频
            # 设置分析参数
            min_conf = 0.25  # 最小置信度
            sensitivity = 1.0  # 灵敏度
            overlap = 0.0  # 重叠度
            
            # 调用BirdNET分析函数
            analyze(
                input=audio_path,
                output=output_path,
                min_conf=min_conf,
                classifier=None,
                lat=-1,
                lon=-1,
                week=-1,
                slist=None,
                sensitivity=sensitivity,
                overlap=overlap,
                rtype='csv'
            )
            
            # 读取分析结果
            detections = []
            if os.path.exists(output_dir):
                # 查找输出目录中的CSV文件
                for file in os.listdir(output_dir):
                    if file.endswith('.csv') and 'BirdNET' in file and 'analysis_params' not in file:
                        csv_path = os.path.join(output_dir, file)
                        with open(csv_path, 'r', encoding='utf-8') as f:
                            lines = f.readlines()
                            self.logger.info(f"CSV file has {len(lines)} lines")
                            if len(lines) > 0:
                                self.logger.info(f"Header: {lines[0].strip()}")
                            
                            # 跳过标题行
                            for i, line in enumerate(lines[1:], 1):
                                parts = line.strip().split(',')
                                self.logger.info(f"Line {i}: {parts}")
                                
                                if len(parts) >= 5:
                                    try:
                                        # CSV format: Start (s),End (s),Scientific name,Common name,Confidence,File
                                        start_time = float(parts[0])
                                        end_time = float(parts[1])
                                        scientific_name = parts[2]
                                        common_name = parts[3]
                                        confidence = float(parts[4])
                                        
                                        detections.append({
                                            'start_time': start_time,
                                            'end_time': end_time,
                                            'species': scientific_name,
                                            'common_name': common_name,
                                            'confidence': confidence
                                        })
                                    except (ValueError, IndexError) as ve:
                                        self.logger.warning(f"Error parsing line {i}: {ve}, parts: {parts}")
                                        continue
                        break
                
                # 清理临时目录
                import shutil
                shutil.rmtree(output_dir)
            
            self.logger.info(f"Found {len(detections)} bird detections")
            return detections
            
        except Exception as e:
            self.logger.error(f"Error analyzing bird audio: {str(e)}")
            return []
    
    def format_bird_tags(self, bird_detections):
        """将BirdNET检测结果格式化为标准标签格式"""
        try:
            # 统计每种鸟类的出现次数
            species_count = {}
            for detection in bird_detections:
                species = detection['species']
                if species in species_count:
                    species_count[species] += 1
                else:
                    species_count[species] = 1
            
            # 转换为标准格式
            formatted_tags = []
            for species, count in species_count.items():
                formatted_tags.append({
                    'species': species,
                    'count': count
                })
            
            return formatted_tags
            
        except Exception as e:
            self.logger.error(f"Error formatting bird tags: {str(e)}")
            return []
     
    def generate_spectrogram(self, y, sr, object_key):
        """生成音频频谱图"""
        try:
            # 限制音频长度以避免超时（最多处理60秒）
            max_duration = 60  # 秒
            max_samples = int(max_duration * sr)
            if len(y) > max_samples:
                y = y[:max_samples]
                self.logger.info(f"Audio truncated to {max_duration} seconds for spectrogram generation")
            
            # 生成梅尔频谱图（降低分辨率以提高速度）
            mel_spec = librosa.feature.melspectrogram(
                y=y, sr=sr, n_mels=64, hop_length=512, n_fft=1024
            )
            mel_spec_db = librosa.power_to_db(mel_spec, ref=np.max)
             
            # 创建图像（降低DPI以提高速度）
            fig, ax = plt.subplots(figsize=(8, 3))
            img = librosa.display.specshow(
                mel_spec_db, sr=sr, x_axis='time', y_axis='mel', ax=ax
            )
            plt.colorbar(img, ax=ax, format='%+2.0f dB')
            plt.title('Mel Spectrogram', fontsize=10)
            plt.tight_layout()
             
            # 保存频谱图到临时文件
            with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as temp_file:
                plt.savefig(temp_file.name, dpi=100, bbox_inches='tight', 
                           facecolor='white', edgecolor='none')
                plt.close(fig)  # 明确关闭图形对象
                plt.clf()  # 清理内存
                return temp_file.name
                 
        except Exception as e:
            self.logger.error(f"Error generating spectrogram: {str(e)}")
            # 确保清理matplotlib资源
            plt.close('all')
            plt.clf()
            raise
     
    def upload_thumbnail(self, thumbnail_path, object_key):
        """上传缩略图到OSS"""
        try:
            # 生成缩略图的OSS key
            base_name = os.path.splitext(os.path.basename(object_key))[0]
            thumbnail_key = f"{self.thumbnail_prefix}{base_name}_spectrogram.png"
             
            # 上传到OSS
            self.bucket.put_object_from_file(thumbnail_key, thumbnail_path)
             
            return thumbnail_key
             
        except Exception as e:
            self.logger.error(f"Error uploading thumbnail: {str(e)}")
            raise
     
    def save_metadata(self, metadata):
        """保存元数据到表格存储"""
        try:
            # 生成唯一的file_id（音频文件格式）
            base_name = os.path.splitext(metadata['file_name'])[0]
            file_id = f"audio_{base_name}_{int(time.time())}"
            
            # 生成13位时间戳
            timestamp = int(time.time() * 1000)
            
            # 构建主键（匹配bird_media_meta表格结构）
            primary_key = [
                ('file_id', file_id),
                ('timestamp', timestamp)
            ]
            
            # 构建属性列（匹配团队数据格式）
            # 将鸟类标签转换为JSON字符串
            tags_json = json.dumps(metadata.get('tags', []), ensure_ascii=False)
            
            attribute_columns = [
                ('oss_url', f"oss://{self.bucket_name}/{metadata['file_path']}"),
                ('file_type', 'audio'),
                ('tags', tags_json),  # 保存鸟类识别结果
                ('user_id', 'audio_processor'),
                ('thumbnail_url', f"oss://{self.bucket_name}/{metadata['thumbnail_path']}"),
                # 额外的音频特定字段
                ('duration', metadata['duration']),
                ('sample_rate', metadata['sample_rate']),
                ('bird_detections', json.dumps(metadata.get('bird_detections', []), ensure_ascii=False)),
                ('processed_time', metadata['processed_time']),
                ('status', metadata['status'])
            ]
            
            # 创建行数据
            row = Row(primary_key, attribute_columns)
            
            # 写入表格存储
            condition = Condition(RowExistenceExpectation.IGNORE)
            self.ots_client.put_row(self.table_name, row, condition)
            
            self.logger.info(f"Metadata saved successfully: file_id={file_id}, timestamp={timestamp}")
            return file_id
            
        except Exception as e:
            self.logger.error(f"Error saving metadata: {str(e)}")
            raise
