"""Refresh the existing Market job from committed source; never execute the job.

Preserve the existing runtime image and all settings except the pinned source.
Cloud API calls use the active gcloud identity and the Python standard library.
"""
import argparse
import copy
import hashlib
import io
import json
from pathlib import Path
import re
import subprocess
import time
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen
import zipfile

ROOT = Path(__file__).resolve().parents[1]
PARENT = 'projects/agm-datalake/locations/us-central1'
JOB = 'agm-market-etl'
FILES = ('src/jobs/__init__.py', 'src/jobs/market_etl.py', 'src/jobs/storage.py',
         'src/utils/ibkr_compliance.py',
         'src/jobs/market_result.py', 'src/components/tools/private/etl.py',
         'src/utils/connectors/ibkr_trading_api.py')


def committed(ref, path):
    return subprocess.check_output(['git', 'show', f'{ref}:{path}'], cwd=ROOT)


def bundle(source):
    output = io.BytesIO()
    with zipfile.ZipFile(output, 'w', zipfile.ZIP_DEFLATED) as archive:
        for path in FILES:
            info = zipfile.ZipInfo(path, (2026, 10, 22, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(info, source(path))
    return output.getvalue()


class Cloud:
    def request(self, url, method='GET', body=None, content_type='application/json'):
        token = subprocess.check_output(['gcloud', 'auth', 'print-access-token'], text=True).strip()
        if isinstance(body, dict):
            body = json.dumps(body).encode()
        request = Request(url, data=body, method=method,
                          headers={'Authorization': f'Bearer {token}', 'Content-Type': content_type})
        with urlopen(request, timeout=60) as response:
            return response.read()

    def json(self, url, **kwargs):
        return json.loads(self.request(url, **kwargs))


def synchronize(cloud, source, commit, apply=False):
    base = f'https://run.googleapis.com/v2/{PARENT}'
    existing = cloud.json(f'{base}/jobs/{JOB}')
    task = existing['template']['template']
    if len(task['containers']) != 1:
        raise ValueError('Expected one Market job container')
    container = task['containers'][0]
    env = {item['name']: item.get('value') for item in container['env']}
    bucket = env['ETL_STORAGE_BUCKET']
    content = bundle(source)
    digest = hashlib.sha256(content).hexdigest()
    name = f'artifacts/market-{digest}.zip'
    image = container['image']
    if not re.fullmatch(r'[^\s@]+@sha256:[a-f0-9]{64}', image):
        raise ValueError('Market runtime must use an immutable image digest')
    result = {'job': JOB, 'commit': commit, 'image': image, 'sha256': digest,
              'mode': 'apply' if apply else 'plan'}
    if not apply:
        return result
    # Never replace a job while its execution or generation-owned lock is active.
    executions = cloud.json(f'{base}/jobs/{JOB}/executions?pageSize=100').get('executions', [])
    if any(not item.get('completionTime') for item in executions):
        raise ValueError('Market execution is active or uncertain; inspect before source update')
    try:
        cloud.request(f'https://storage.googleapis.com/storage/v1/b/{bucket}/o/locks%2Fmarket-etl.json')
    except HTTPError as error:
        if error.code != 404:
            raise
    else:
        raise ValueError('Market lock exists; inspect before source update')
    encoded = quote(name, safe='')
    try:
        uploaded = cloud.json(f'https://storage.googleapis.com/upload/storage/v1/b/{bucket}/o'
                              f'?uploadType=media&name={encoded}&ifGenerationMatch=0',
                              method='POST', body=content, content_type='application/zip')
    except HTTPError as error:
        if error.code != 412:
            raise
        uploaded = cloud.json(f'https://storage.googleapis.com/storage/v1/b/{bucket}/o/{encoded}')
    generation = str(uploaded['generation'])
    stored = cloud.request(f'https://storage.googleapis.com/storage/v1/b/{bucket}/o/{encoded}'
                           f'?alt=media&generation={generation}')
    if stored != content:
        raise ValueError('Uploaded Market source verification failed')
    template = copy.deepcopy(existing['template'])
    candidate = template['template']['containers'][0]
    candidate['image'] = image
    candidate['command'] = ['python']
    candidate['args'] = ['-c', source('src/jobs/market_bootstrap.py').decode()]
    changes = {'ETL_BUNDLE_NAME': name, 'ETL_BUNDLE_GENERATION': generation,
               'ETL_BUNDLE_SHA256': digest}
    for item in candidate['env']:
        if item['name'] in changes:
            item['value'] = changes[item['name']]
    # Preserve all other settings and use the existing etag to reject concurrent edits.
    body = {key: copy.deepcopy(existing[key]) for key in (
        'name', 'etag', 'annotations', 'launchStage', 'binaryAuthorization', 'client', 'clientVersion'
    ) if key in existing}
    body.update(template=template, labels={**existing.get('labels', {}), 'source-commit': commit})
    operation = cloud.json(f'{base}/jobs/{JOB}', method='PATCH', body=body)
    for _ in range(60):
        if operation.get('done'):
            break
        time.sleep(2)
        operation = cloud.json('https://run.googleapis.com/v2/' + operation['name'])
    if not operation.get('done') or operation.get('error'):
        raise RuntimeError('Market job update did not complete successfully; inspect Cloud Run operation')
    actual = cloud.json(f'{base}/jobs/{JOB}')
    if actual['template'] != template or actual.get('labels', {}).get('source-commit') != commit:
        raise RuntimeError('Market job source/configuration readback did not match')
    return {**result, 'bundle': name, 'generation': generation, 'verified': True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-ref', default='HEAD')
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    commit = subprocess.check_output(['git', 'rev-parse', '--verify', args.source_ref + '^{commit}'],
                                     cwd=ROOT, text=True).strip()
    print(json.dumps(synchronize(Cloud(), lambda path: committed(commit, path), commit,
                                 apply=args.apply)))


if __name__ == '__main__':
    main()
