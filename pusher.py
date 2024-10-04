import requests as rq

api_url = 'http://10.4.178.173:8080'

one = rq.post(api_url + '/login', json={
  'username': 'admin',
  'password': 'password'
})

response = rq.post(api_url + '/database/read', json={
  'path': 'db/clients/tickets',
}, headers={
  'Authorization': f'Bearer {one.json()["access_token"]}'
})

print(response.json())