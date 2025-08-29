# FIT5225-Assignment-3

## User Guide (Web UI)

This repository includes a simple static Web UI for BirdTag, supporting file upload, query, analysis, and deletion via API Gateway. No subscription/notification features are included in this UI.

### Quick Start

1. **Open the UI**
	- Open `webui/index.html` in your browser (recommended: use VS Code Live Server or `python3 -m http.server -d webui 5500`).

2. **Configure API Gateway and AppCodes**
	- At the top of the page, fill in:
	  - API Gateway Domain (e.g. `http://www.fit5032-a3.online` or `http://127.0.0.1:5050` for local proxy)
	  - AppCode for each interface (Query, Analysis, Delete, Upload)
	- Click "Save Config" or "Reset Defaults" to auto-fill known AppCodes.

3. **File Upload**
	- Select a file (image/audio/video). For images, "Compress Large Images" is enabled by default to avoid payload too large errors.
	- Click "Upload to OSS". The result will show the OSS URL if successful.
	- For troubleshooting, use "Test Upload (small text)" to verify connectivity.

4. **Query & Analysis**
	- Fill in species, species_not, count_min, count_max, minimum_should_match as needed.
	- Click "Analysis" or "Query". Results are shown as cards; click a card to auto-fill the delete form.

5. **Delete**
	- After selecting a result, verify the delete form and click "Delete" to remove both OSS file and TableStore record.

### Troubleshooting

- If you see 400 Bad Request on upload, try enabling image compression or use the local proxy (`python3 webui/optional_proxy.py`) and set API Gateway Domain to `http://127.0.0.1:5050`.
- For CORS issues, always use the local proxy for browser access.
- Make sure AppCodes are correct and saved before using each function.

### Advanced

- The UI auto-converts `oss://bucket/key` to direct HTTPS links (region defaults to oss-cn-hongkong).
- All requests use minimal headers (Authorization, Content-Type) for compatibility with API Gateway.
- For large files or advanced upload, consider direct OSS multipart upload (not included in this UI).

### Contact

For backend/API issues, refer to the function code and API Gateway documentation. For UI bugs, check `webui/main.js` and `webui/README.md`.