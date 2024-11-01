import requests
import os

url = f'http://0.0.0.0:{os.getenv("API_PORT")}'

def access_api(endpoint, method='GET', data=None):
    auth = requests.post(url + '/login', json={
        'username': 'admin',
        'password': 'password'
    })
    response = requests.request(method, url + endpoint, json=data, headers={
        'Authorization': f'Bearer {auth.json()["access_token"]}'
    })
    try:
        return response.json()
    except:
        return response.content