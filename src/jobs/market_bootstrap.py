"""Job command: load a SHA-verified immutable overlay into the existing API image."""
import hashlib
import io
import os
from pathlib import Path
import runpy
import sys
import zipfile
import google.auth
from google.auth.transport.requests import AuthorizedSession

ALLOWED = {'src/jobs/__init__.py', 'src/jobs/market_etl.py', 'src/jobs/storage.py',
           'src/utils/ibkr_compliance.py',
           'src/jobs/market_result.py', 'src/components/tools/private/etl.py',
           'src/utils/connectors/ibkr_trading_api.py'}


def main():
    credentials, _ = google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
    session = AuthorizedSession(credentials)
    from urllib.parse import quote
    bucket = os.environ['ETL_STORAGE_BUCKET']
    name = quote(os.environ['ETL_BUNDLE_NAME'], safe='')
    response = session.get(f'https://storage.googleapis.com/storage/v1/b/{bucket}/o/{name}',
        params={'alt': 'media', 'generation': os.environ['ETL_BUNDLE_GENERATION']}, timeout=60)
    response.raise_for_status()
    content = response.content
    if hashlib.sha256(content).hexdigest() != os.environ['ETL_BUNDLE_SHA256']:
        raise RuntimeError('ETL source checksum mismatch')
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        if set(archive.namelist()) != ALLOWED or len(archive.namelist()) != len(ALLOWED):
            raise RuntimeError('Unexpected ETL bundle contents')
        for name in archive.namelist():
            target = Path('/app') / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(archive.read(name))
            target.chmod(0o600)
    os.chdir('/app')
    sys.path.insert(0, '/app')
    sys.argv = ['market_etl', '--output-dir', '/app/cache/market-run', '--storage-bucket', bucket]
    runpy.run_module('src.jobs.market_etl', run_name='__main__')


if __name__ == '__main__':
    main()
