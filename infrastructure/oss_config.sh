#!/bin/bash

BUCKET_NAME="fit5225-a3"
REGION="cn-hongkong"
FRONTEND_DOMAIN="*"

# 配置CORS规则（修正参数格式）
aliyun oss cors put --bucket "$BUCKET_NAME" --region "$REGION" <<EOF
[
  {
    "AllowedOrigins": ["$FRONTEND_DOMAIN"],
    "AllowedMethods": ["GET", "POST", "PUT"],
    "AllowedHeaders": ["Authorization", "Content-Type"],
    "ExposeHeaders": [],
    "MaxAgeSeconds": 300
  }
]
EOF

