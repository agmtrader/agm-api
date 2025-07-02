import requests

url = "http://localhost:5000"

token = requests.post(url + "/token", json={"token": "test", "scopes": "all"}).json()
print(token)