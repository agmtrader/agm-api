import requests as rq

url = 'http://127.0.0.1:5001'

response = rq.post(url + '/reporting/generate')

print(response.json())