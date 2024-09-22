import requests as rq

url = 'http://127.0.0.1:5001'

response = rq.post(url + '/drive/get_files_in_folder', json={'parent_id': '1iq3WW7TFzxL8RkTZcFp2qyELuqZPBB8T'}).json()
clients_files = response['content']

print(f'Clients files: {clients_files}')