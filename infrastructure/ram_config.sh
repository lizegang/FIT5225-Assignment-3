#!/bin/bash

USER_NAME="birdtag-user"
USER_PASSWORD="ComplexPassword123!"  # 确保符合阿里云密码策略（字母+数字+符号）

# 替换为你的阿里云账号信息
REGION="cn-hongkong"
ACCOUNT_ID="1864950659213545"  # 移除尖括号，填写实际16位账号ID
BUCKET_NAME="fit5225-a3"

# 函数计算信息（如果未创建，先注释下面两行）
# FC_SERVICE_NAME="你的函数服务名"
# FC_IMAGE_FUNCTION="你的图像处理函数名"

# 创建RAM用户（修正密码参数）
aliyun ram CreateUser \
  --UserName "$USER_NAME" \
  --LoginProfile "{\"Password\":\"$USER_PASSWORD\", \"PasswordResetRequired\":false}"

# 创建RAM策略（修正命令和资源路径）
POLICY_CONTENT=$(cat <<EOF
{
  "Version": "1",
  "Statement": [
    {
      "Action": ["oss:PutObject", "oss:GetObject"],
      "Resource": ["acs:oss:$REGION:$ACCOUNT_ID:$BUCKET_NAME/*"],
      "Effect": "Allow"
    }
    # 若未创建函数计算，注释下面的FC权限段
    # ,{
    #   "Action": ["fc:InvokeFunction"],
    #   "Resource": ["acs:fc:$REGION:$ACCOUNT_ID:services/$FC_SERVICE_NAME/functions/$FC_IMAGE_FUNCTION"],
    #   "Effect": "Allow"
    # }
  ]
}
EOF
)

aliyun ram CreatePolicy \
  --PolicyName "BirdTagPolicy" \
  --PolicyDocument "$POLICY_CONTENT" \
  --Description "BirdTag权限策略"

# 绑定策略到用户
aliyun ram AttachPolicyToUser \
  --UserName "$USER_NAME" \
  --PolicyName "BirdTagPolicy" \
  --PolicyType Custom
