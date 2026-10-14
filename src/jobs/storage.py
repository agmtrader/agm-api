"""Private GCS JSON reports and generation-conditional execution exclusion."""
import json
from urllib.parse import quote
import google.auth
from google.auth.transport.requests import AuthorizedSession


class RunStorage:
    def __init__(self, bucket):
        credentials, _ = google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
        self.session = AuthorizedSession(credentials)
        self.bucket = bucket

    def put(self, name, data):
        response = self.session.post(
            f'https://storage.googleapis.com/upload/storage/v1/b/{self.bucket}/o',
            params={'uploadType': 'media', 'name': name, 'ifGenerationMatch': '0'},
            headers={'Content-Type': 'application/json'}, data=json.dumps(data).encode(), timeout=30)
        if response.status_code not in (200, 201):
            raise RuntimeError(f'GCS write {name}: HTTP {response.status_code}')
        return response.json()['generation']

    def delete(self, name, generation):
        response = self.session.delete(
            f'https://storage.googleapis.com/storage/v1/b/{self.bucket}/o/{quote(name, safe="")}',
            params={'ifGenerationMatch': generation}, timeout=30)
        if response.status_code != 204:
            raise RuntimeError(f'GCS release lock: HTTP {response.status_code}')
