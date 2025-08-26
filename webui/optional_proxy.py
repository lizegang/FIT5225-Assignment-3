# -*- coding: utf-8 -*-
"""
简单反向代理，便于本地前端调试时绕过浏览器 CORS 限制。
启动：
  pip install flask requests
  python webui/optional_proxy.py
前端将 baseUrl 配置为 http://127.0.0.1:5050 即可。
"""
from flask import Flask, request, jsonify, Response
import requests
import os

app = Flask(__name__)
TARGET = os.environ.get('BIRDTAG_TARGET', 'http://www.fit5032-a3.online')

@app.route('/', defaults={'path': ''}, methods=['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS'])
@app.route('/<path:path>', methods=['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS'])
def proxy(path):
    url = TARGET.rstrip('/') + '/' + path
    headers = dict(request.headers)
    # 允许浏览器跨域
    resp = requests.request(
        method=request.method,
        url=url,
        headers=headers,
        data=request.get_data(),
        params=request.args,
    )
    excluded = ['content-encoding', 'content-length', 'transfer-encoding', 'connection']
    out_headers = [(name, value) for (name, value) in resp.raw.headers.items() if name.lower() not in excluded]
    out = Response(resp.content, status=resp.status_code)
    for k, v in out_headers:
        out.headers[k] = v
    out.headers['Access-Control-Allow-Origin'] = '*'
    out.headers['Access-Control-Allow-Headers'] = '*'
    out.headers['Access-Control-Allow-Methods'] = 'GET,POST,PUT,PATCH,DELETE,OPTIONS'
    return out

if __name__ == '__main__':
    app.run('127.0.0.1', 5050, debug=True)
