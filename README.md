# FIT5225-Assignment-3

本仓库包含 BirdTag 项目的函数计算、查询接口与本地 Web UI（简易）。

## Web UI（成员6，暂不含订阅）
目录：`webui/`，纯静态前端，可直接用浏览器打开 `webui/index.html`。

- 顶部可配置：API 网关域名与各接口 AppCode。
- 支持：上传（/upload）、analysis（/analysis）、查询（/query）、删除（/delete）。
- 查询结果以卡片展示，点击卡片可自动填充删除表单。

如遇浏览器 CORS：
1) 可选启动本地反向代理（需 Python 3.8+）：
	- 安装依赖：`pip install -r webui/requirements.txt`
	- 启动：`python webui/optional_proxy.py`
	- 页面“API 网关域名”填 `http://127.0.0.1:5050`
2) 或使用 VS Code Live Server/静态服务器并确保网关允许跨域。

更多说明见 `webui/README.md`。