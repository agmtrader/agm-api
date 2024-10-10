import requests as rq

api_url = 'http://127.0.0.1:8080'

one = rq.post(api_url + '/login', json={
  'username': 'admin',
  'password': 'password'
})

response = rq.post(api_url + '/database/list_subcollections', json={
  'parent_id': 'db/clients',
}, headers={
  'Authorization': f'Bearer {one.json()["access_token"]}'
})

print(response.json())