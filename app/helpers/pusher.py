import requests as rq
import os

from dotenv import load_dotenv
load_dotenv()

full_url = f'{os.getenv("API_URL")}:{os.getenv("API_PORT")}'

one = rq.post(full_url + '/login', json={
  'username': os.getenv('AUTH_USERNAME'),
  'password': os.getenv('AUTH_PASSWORD')
})
print(one)

response = rq.get(full_url + '/reporting/load', headers={
  'Authorization': f'Bearer {one.json()["access_token"]}'
})
print(response.json())