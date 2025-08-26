const $ = (sel) => document.querySelector(sel);
const LS_KEY = 'birdtag_env';
const DEFAULTS = {
  baseUrl: 'http://www.fit5032-a3.online',
  appcodeQuery: '648e7efb47e747d1be2de5039150c834',
  appcodeAnalysis: '38f6729313934179b7eb6a46476bd431',
  appcodeDelete: 'e2e0791324e5409693308a4b7aedf1f4',
  appcodeUpload: 'ce26efc94ef94575bec234e71861af5f'
};

function loadEnv() {
  let env = {};
  try { env = JSON.parse(localStorage.getItem(LS_KEY) || '{}'); } catch { env = {}; }
  if (!env.baseUrl && !env.appcodeQuery && !env.appcodeAnalysis && !env.appcodeDelete && !env.appcodeUpload) {
    // 首次加载或无配置，填入默认值
    env = { ...DEFAULTS };
    try {
      if (typeof location !== 'undefined' && (location.hostname === '127.0.0.1' || location.hostname === 'localhost')) {
        env.baseUrl = 'http://127.0.0.1:5050'; // 本地调试优先走代理，规避 CORS
      }
    } catch {}
    localStorage.setItem(LS_KEY, JSON.stringify(env));
  }
  $('#baseUrl').value = env.baseUrl || DEFAULTS.baseUrl;
  $('#appcodeQuery').value = env.appcodeQuery || DEFAULTS.appcodeQuery;
  $('#appcodeAnalysis').value = env.appcodeAnalysis || DEFAULTS.appcodeAnalysis;
  $('#appcodeDelete').value = env.appcodeDelete || DEFAULTS.appcodeDelete;
  $('#appcodeUpload').value = env.appcodeUpload || DEFAULTS.appcodeUpload;
}

function saveEnv() {
  const env = {
    baseUrl: $('#baseUrl').value.trim(),
    appcodeQuery: $('#appcodeQuery').value.trim(),
    appcodeAnalysis: $('#appcodeAnalysis').value.trim(),
    appcodeDelete: $('#appcodeDelete').value.trim(),
    appcodeUpload: $('#appcodeUpload').value.trim()
  };
  localStorage.setItem(LS_KEY, JSON.stringify(env));
}

function getEnv() {
  try { return JSON.parse(localStorage.getItem(LS_KEY) || '{}'); } catch { return { ...DEFAULTS }; }
}

function headersFor(appcode) {
  return {
    'Authorization': 'APPCODE ' + (appcode || ''),
    'Content-Type': 'application/json',
  };
}

async function callApi(path, appcode, body) {
  const env = getEnv();
  const url = env.baseUrl.replace(/\/$/, '') + path;
  try {
    const resp = await fetch(url, {
      method: 'POST',
      headers: headersFor(appcode),
      body: JSON.stringify(body || {})
    });
    const text = await resp.text();
    if (!resp.ok) {
      return { error: true, status: resp.status, statusText: resp.statusText, body: text || null };
    }
    if (!text) {
      return { warning: 'empty_body', note: '响应体为空，可能为网关配置或 CORS 影响', raw: '' };
    }
    try { return JSON.parse(text); } catch { return { raw: text }; }
  } catch (e) {
    return { error: true, message: String(e) };
  }
}

function parseCSV(str) {
  return str.split(',').map(s => s.trim()).filter(Boolean);
}

function renderList(items = []) {
  const grid = $('#grid');
  grid.innerHTML = '';
  items.forEach(it => {
    const div = document.createElement('div');
    div.className = 'item';
    const title = it.file_id ? `${it.file_id} @ ${it.timestamp}` : (it.oss_url || 'item');
    const thumb = it.thumbnail_url ? `<img class="thumb" src="${toHttp(it.thumbnail_url)}" alt="thumb" />` : '';
    div.innerHTML = `
      <h4>${title}</h4>
      ${thumb}
      <a href="${toHttp(it.oss_url)}" target="_blank">${toHttp(it.oss_url)}</a>
      <div class="meta">${Array.isArray(it.tags) ? JSON.stringify(it.tags) : (it.tags || '')}</div>
    `;
    div.addEventListener('click', () => fillDeleteForm(it));
    grid.appendChild(div);
  });
}

function toHttp(ossUrl) {
  if (!ossUrl) return '';
  // very naive mapping: oss://bucket/key -> https://bucket.oss-cn-hongkong.aliyuncs.com/key
  try {
    const m = /^oss:\/\/([^/]+)\/(.+)$/.exec(ossUrl);
    if (!m) return ossUrl;
    const bucket = m[1];
    const key = m[2];
    const region = 'oss-cn-hongkong';
    return `https://${bucket}.${region}.aliyuncs.com/${key}`;
  } catch { return ossUrl; }
}

function fillDeleteForm(it) {
  $('#tableName').value = 'bird_media_meta';
  if (it.file_id) $('#fileId').value = it.file_id;
  if (it.timestamp) $('#timestamp').value = it.timestamp;
  if (it.oss_url) $('#ossUrl').value = it.oss_url;
  if (it.thumbnail_url) $('#thumbUrl').value = it.thumbnail_url;
}

async function onUpload() {
  const env = getEnv();
  const file = $('#fileInput').files[0];
  if (!file) { $('#uploadResult').textContent = '请选择文件'; return; }
  $('#uploadInfo').textContent = `${file.name} · ${(file.size/1024).toFixed(1)} KB`;
  const reader = new FileReader();
  reader.onload = async () => {
    let dataUrl = reader.result;
    if ($('#compress').checked && file.type.startsWith('image/')) {
      try {
        dataUrl = await compressImageDataUrl(dataUrl, 1280, 0.8); // 宽度压到1280，质量 0.8
      } catch (e) {
        console.warn('compress failed:', e);
      }
    }
    const base64 = dataUrl.split(',')[1];
    const body = { file_name: file.name, file_content_base64: base64 };
    $('#uploadInfo').textContent += ` -> payload ${(base64.length/1024).toFixed(1)} KB`;
    const data = await callApi('/upload', env.appcodeUpload, body);
    $('#uploadResult').textContent = JSON.stringify(data, null, 2);
  };
  reader.readAsDataURL(file);
}

async function onAnalysis() {
  const env = getEnv();
  const body = buildQueryBody();
  const data = await callApi('/analysis', env.appcodeAnalysis, body);
  $('#queryResult').textContent = JSON.stringify(data, null, 2);
  const list = Array.isArray(data?.data) ? data.data : (Array.isArray(data?.rows) ? data.rows : []);
  renderList(list);
}

async function onQuery() {
  const env = getEnv();
  const body = buildQueryBody();
  const data = await callApi('/query', env.appcodeQuery, body);
  $('#queryResult').textContent = JSON.stringify(data, null, 2);
  const list = Array.isArray(data?.data) ? data.data : (Array.isArray(data?.rows) ? data.rows : []);
  renderList(list);
}

function buildQueryBody() {
  const species = parseCSV($('#species').value);
  const species_not = parseCSV($('#speciesNot').value);
  const count_min = parseInt($('#countMin').value || '1', 10);
  const count_max = parseInt($('#countMax').value || '2000000000', 10);
  const minimum_should_match = parseInt($('#msm').value || '1', 10);
  return { species, species_not, count_min, count_max, minimum_should_match };
}

async function onDelete() {
  const env = getEnv();
  const body = {
    table_name: $('#tableName').value.trim(),
    file_id: $('#fileId').value.trim(),
    timestamp: isNaN(parseInt($('#timestamp').value, 10)) ? $('#timestamp').value.trim() : parseInt($('#timestamp').value, 10),
    oss_url: $('#ossUrl').value.trim(),
    thumbnail_url: $('#thumbUrl').value.trim(),
  };
  const data = await callApi('/delete', env.appcodeDelete, body);
  $('#deleteResult').textContent = JSON.stringify(data, null, 2);
}

function init() {
  loadEnv();
  $('#saveEnv').addEventListener('click', saveEnv);
  $('#resetEnv').addEventListener('click', () => {
    localStorage.setItem(LS_KEY, JSON.stringify(DEFAULTS));
    loadEnv();
  });
  $('#btnUpload').addEventListener('click', onUpload);
  $('#btnAnalysis').addEventListener('click', onAnalysis);
  $('#btnQuery').addEventListener('click', onQuery);
  $('#btnDelete').addEventListener('click', onDelete);
}

document.addEventListener('DOMContentLoaded', init);

// 将 dataURL 的图片压缩到指定宽度与质量
function compressImageDataUrl(dataUrl, maxWidth = 1280, quality = 0.8) {
  return new Promise((resolve, reject) => {
    const img = new Image();
    img.onload = () => {
      const scale = Math.min(1, maxWidth / img.width);
      const w = Math.round(img.width * scale);
      const h = Math.round(img.height * scale);
      const canvas = document.createElement('canvas');
      canvas.width = w; canvas.height = h;
      const ctx = canvas.getContext('2d');
      ctx.drawImage(img, 0, 0, w, h);
      const out = canvas.toDataURL('image/jpeg', quality);
      resolve(out);
    };
    img.onerror = reject;
    img.src = dataUrl;
  });
}

// 绑定测试上传：发送一个很小的 base64 文本，验证链路
document.addEventListener('DOMContentLoaded', () => {
  const btn = document.getElementById('btnUploadTest');
  if (!btn) return;
  btn.addEventListener('click', async () => {
    const env = getEnv();
    const body = { file_name: 'hello.txt', file_content_base64: btoa('hello birdtag') };
    const data = await callApi('/upload', env.appcodeUpload, body);
    document.getElementById('uploadResult').textContent = JSON.stringify(data, null, 2);
  });
});
