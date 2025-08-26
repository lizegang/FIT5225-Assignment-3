# BirdTag Web UI (临时版，无订阅)

此 Web UI 仅用于演示与调试现有 API 网关能力：上传、查询、analysis 转发、删除。暂不实现订阅功能。

## 功能
- 配置 API 网关域名与各接口 AppCode
- 上传文件到 OSS（经 API 网关 -> FC -> OSS）
- 多条件查询与 analysis 调用
- 点选结果后可一键填充删除表单并删除 OSS + 表格存储记录

## 目录
- index.html, styles.css, main.js: 纯前端，无框架
- optional_proxy.py: 可选的本地 Flask 反向代理，解决浏览器 CORS 问题

## 运行
直接用 VS Code Live Server 或本地静态服务打开 `index.html`。如遇到 CORS：

1) 启动本地代理（可选）
- 需要 Python 3.8+
- 安装依赖：
  pip install flask requests
- 启动：
  python webui/optional_proxy.py
- 代理地址： http://127.0.0.1:5050

2) 在页面顶部填写：
- API 网关域名：如 http://www.fit5032-a3.online
- 各接口 AppCode（按团队分配）
- 保存配置

3) 使用
- 上传：选择文件 -> 上传
- 查询：填写 species/count 区间 -> 点击“先解析(analysis)”或“直接查询(query)”
- 删除：从查询结果卡片点击可自动填充删除表单 -> 点击“删除”

注意：oss:// URL 已在前端做了简单的 https 直链转换，区域默认 oss-cn-hongkong，可按需改 `main.js` 的 toHttp。
