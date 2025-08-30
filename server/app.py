from flask import Flask, request, jsonify
from flask_cors import CORS
import smtplib
import requests
from email.mime.text import MIMEText
from email.header import Header
import json
import os

app = Flask(__name__)

def load_users():
    file_path = os.path.join(os.path.dirname(__file__), './data/users.json')
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data['users']

def load_admins():
    file_path = os.path.join(os.path.dirname(__file__), './data/admins.json')
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data['admins']

def save_users(users):
    file_path = os.path.join(os.path.dirname(__file__), './data/users.json')
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump({'users': users}, f, indent=2, ensure_ascii=False)

def load_resources():
    file_path = os.path.join(os.path.dirname(__file__), './data/resources.json')
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data

@app.route('/submit_rating', methods=['POST'])
def submit_rating():
    try:
        data = request.get_json()
        resource_id = data.get("id")
        rating = data.get("rating")  
        
        if not resource_id or not rating:
            return jsonify({"status": "fail", "error": "Missing ID or rating"}), 200

        resources_data = load_resources()
        updated = False

        rating_map = {
            "Excellent": 0,
            "Good": 1,
            "General": 2,
            "Unlike": 3,
            "I don't care": 4
        }

        for item in resources_data["resources"]:
            if str(item["id"]) == str(resource_id):
                for key in rating:
                    index = rating_map.get(rating[key])
                    if index is not None and key.capitalize() in item:
                        item[key.capitalize()][index] += 1
                updated = True
                break

        if updated:
            file_path = os.path.join(os.path.dirname(__file__), './data/resources.json')
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(resources_data, f, indent=2, ensure_ascii=False)
            return jsonify({"status": "success"}), 200
        else:
            return jsonify({"status": "fail", "error": "Resource not found"}), 200

    except Exception as e:
        return jsonify({"status": "fail", "error": str(e)}), 200

@app.route('/userlogin', methods=['POST'])
def login():
    data = request.get_json()

    if not data or 'account' not in data or 'password' not in data:
        return jsonify({"error": "Missing account or password"}), 200

    account = data['account']
    password = data['password']
    users = load_users()

    for user in users:
        if user['account'] == account and user['password'] == password:
            user_info = {k: v for k, v in user.items() if k != 'password'}
            return jsonify({"status": "success", "user": user_info}), 200

    return jsonify({"status": "fail", "error": "Invalid account or password"}), 200

@app.route('/adminlogin', methods=['POST'])
def admin_login():
    data = request.get_json()

    if not data or 'account' not in data or 'password' not in data:
        return jsonify({"error": "Missing account or password"}), 200

    account = data['account']
    password = data['password']
    admins = load_admins()

    for admin in admins:
        if admin['account'] == account and admin['password'] == password:
            admin_info = {k: v for k, v in admin.items() if k != 'password'}
            return jsonify({"status": "success", "admin": admin_info}), 200

    return jsonify({"status": "fail", "error": "Invalid account or password"}), 400

@app.route('/send_register_email', methods=['POST'])
def send_register_email(to_email, username):
    smtp_server = 'smtp.qq.com'
    smtp_port = 465
    sender_email = '3439626328@qq.com'
    sender_pass = 'zjshohskkgnvdaje'  # Google App Password

    subject = 'Registration Successful'
    content = f'Dear {username},\n\nCongratulations! Your registration was successful. Welcome to our platform.'

    message = MIMEText(content, 'plain', 'utf-8')
    message['From'] = Header(sender_email)
    message['To'] = Header(to_email)
    message['Subject'] = Header(subject)

    try:
        server = smtplib.SMTP_SSL(smtp_server, smtp_port)
        server.login(sender_email, sender_pass)
        server.sendmail(sender_email, [to_email], message.as_string())
        server.quit()
        return True
    except Exception as e:
        print(f"Failed to send email: {e}")
        return False

@app.route('/download_all_user_info', methods=['GET'])
def download_all_user_info():
    users = load_users()
    users_info = [{k: v for k, v in user.items() if k != "password"} for user in users]
    return jsonify({"status": "success", "users": users_info}), 200

@app.route('/register', methods=['POST'])
def register():
    data = request.get_json()
    if not data or 'account' not in data or 'username' not in data or 'password' not in data:
        return jsonify({"status": "fail", "error": "Missing registration fields"}), 200

    account = data['account']
    print(account)
    username = data['username']
    password = data['password']
    
    users = load_users()

    for user in users:
        if user['account'] == account:
            return jsonify({"status": "fail", "error": "Email already exists"}), 200
    if users:
        max_id = max(int(user['id']) for user in users)
    else:
        max_id = 0
    new_id = str(max_id + 1).zfill(3)

    default_icon = "/static/images/Default_Avatar.svg"

    new_user = {
        "id": new_id,
        "username": username,
        "account": account,
        "password": password,
        "icon": default_icon
    }

    users.append(new_user)
    save_users(users)

    response = requests.post('http://email-service-kargorbwct.cn-hongkong.fcapp.run', data={
        'to_email': account,
        'username': username
    })
    user_info = {k: v for k, v in new_user.items() if k != 'password'}

    return jsonify({"status": "success", "user": user_info}), 200

@app.route('/get_resource', methods=['POST'])
def get_content():
    try:
        content_data = load_resources()
        return jsonify({"status": "success", "data": content_data}), 200
    except Exception as e:
        return jsonify({"status": "fail", "error": str(e)}), 200

if __name__ == '__main__':
    CORS(app, resources={r"/*": {"origins": "http://localhost:5173"}}, supports_credentials=True)
    app.run(port=5000)
