import requests
import os

url = os.getenv('API_URL')
port = os.getenv('API_PORT')
full_url = f'{url}:{port}'

def access_api(endpoint, method='GET', data=None):
    auth = requests.post(full_url + '/login', json={
        'username': 'admin',
        'password': 'password'
    })
    response = requests.request(method, full_url + endpoint, json=data, headers={
        'Authorization': f'Bearer {auth.json()["access_token"]}'
    })
    try:
        return response.json()
    except:
        return response.content