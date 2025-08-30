import requests

BASE_URL = 'http://fit-spmnrkxajd.cn-hongkong.fcapp.run/userlogin'

test_data = {
    "username": "corey",
    "password": "123456"
}

response = requests.post(BASE_URL, json=test_data)

if response.status_code == 200:
    print("sucess:")
    print(response.json())
elif response.status_code == 401:
    print("fail:")
    print(response.json())
else:
    print(f"error:{response.status_code}")
    print(response.text)
