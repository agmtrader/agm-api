import requests
import os

url = os.getenv('API_URL')

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