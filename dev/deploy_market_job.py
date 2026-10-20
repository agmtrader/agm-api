"""Prepare/apply the market job using the current immutable API runtime; never execute it."""
import argparse
import hashlib
import io
import json
from pathlib import Path
import sys
from urllib.parse import quote
import zipfile

import google.auth
from google.auth.transport.requests import AuthorizedSession

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
PARENT = 'projects/agm-datalake/locations/us-central1'
BUCKET = 'agm-datalake-etl-runs'
JOB = 'agm-market-etl'
FILES = ('src/jobs/__init__.py', 'src/jobs/market_etl.py', 'src/jobs/storage.py',
         'src/jobs/market_result.py', 'src/components/tools/private/etl.py')


def bundle():
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED) as archive:
        for name in FILES:
            # Reproducible source artifact, independent of local file mtimes.
            info = zipfile.ZipInfo(name, (2026, 10, 20, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(info, (ROOT / name).read_bytes())
    return buffer.getvalue()


def configuration(template, stage, name, generation, digest):
    image = template['containers'][0]['image']
    if '@sha256:' not in image:
        raise ValueError('Current API runtime must have an immutable image digest')
    if stage in ('1N3LwrG7IossvCrrrFufWMb26VOcRxhi8', '1EIshGq22oFg-WJ9aHdS37Heh5KaqN2ct'):
        raise ValueError('Market staging must be separate from shared and clients inputs')
    vpc = dict(template['vpcAccess'])
    if not vpc['connector'].startswith('projects/'):
        vpc['connector'] = PARENT + '/connectors/' + vpc['connector']
    env = {'DEV_MODE': 'false', 'ETL_BATCH_FOLDER_ID': stage, 'ETL_STORAGE_BUCKET': BUCKET,
           'ETL_BUNDLE_NAME': name, 'ETL_BUNDLE_GENERATION': str(generation), 'ETL_BUNDLE_SHA256': digest}
    return {'labels': {'purpose': 'market-etl', 'managed-by': 'codex'},
            'template': {'taskCount': 1, 'parallelism': 1, 'template': {
                'serviceAccount': template['serviceAccount'], 'timeout': '3600s', 'maxRetries': 0,
                'vpcAccess': vpc, 'containers': [{'image': image, 'command': ['python'],
                    'args': ['-c', (ROOT / 'src/jobs/market_bootstrap.py').read_text()],
                    'env': [{'name': key, 'value': value} for key, value in env.items()],
                    'resources': {'limits': {'cpu': '1', 'memory': '2Gi'}}}]}}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--staging-folder', required=True)
    parser.add_argument('--apply', action='store_true', help='Upload artifact and create/update the job; never execute')
    parser.add_argument('--update', action='store_true')
    args = parser.parse_args()
    credentials, _ = google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
    session = AuthorizedSession(credentials)
    response = session.get(f'https://run.googleapis.com/v2/{PARENT}/services/agm-api', timeout=30)
    response.raise_for_status()
    template = response.json()['template']
    content = bundle()
    digest = hashlib.sha256(content).hexdigest()
    name = f'artifacts/market-{digest}.zip'
    job = configuration(template, args.staging_folder, name, 'pending', digest)
    if not args.apply:
        print(json.dumps({'mode': 'plan', 'job': JOB, 'image': template['containers'][0]['image'],
                          'stage': args.staging_folder, 'sha256': digest, 'files': FILES}))
        return
    # Validate actual Drive folder and empty staging before any provisioning mutation.
    from src.utils.connectors.drive import GoogleDrive
    drive = GoogleDrive()
    drive._ensure_fresh_connection()
    folder = drive.service.files().get(fileId=args.staging_folder, fields='id,mimeType,trashed', supportsAllDrives=True).execute()
    if folder.get('trashed') or folder.get('mimeType') != 'application/vnd.google-apps.folder' or drive.get_files_in_folder(args.staging_folder):
        raise ValueError('Expected an empty dedicated market staging folder')
    base = f'https://run.googleapis.com/v2/{PARENT}/jobs'
    existing = session.get(f'{base}/{JOB}', timeout=30)
    if existing.status_code not in (200, 404):
        existing.raise_for_status()
    if (existing.status_code == 200) != args.update:
        raise ValueError('Existing job requires --update; new job must omit --update')
    upload = session.post(f'https://storage.googleapis.com/upload/storage/v1/b/{BUCKET}/o',
        params={'uploadType': 'media', 'name': name, 'ifGenerationMatch': '0'},
        headers={'Content-Type': 'application/zip'}, data=content, timeout=60)
    if upload.status_code == 412:
        upload = session.get(f'https://storage.googleapis.com/storage/v1/b/{BUCKET}/o/{quote(name, safe="")}', timeout=30)
    upload.raise_for_status()
    generation = upload.json()['generation']
    job = configuration(template, args.staging_folder, name, generation, digest)
    if args.update:
        job['etag'] = existing.json()['etag']
        response = session.patch(f'{base}/{JOB}', params={'updateMask': 'template,labels'}, json=job, timeout=60)
    else:
        response = session.post(base, params={'jobId': JOB}, json=job, timeout=60)
    response.raise_for_status()
    print(json.dumps({'operation': response.json()['name'], 'job': JOB, 'image': template['containers'][0]['image'],
                      'bundle': name, 'generation': generation, 'sha256': digest, 'stage': args.staging_folder}))


if __name__ == '__main__':
    main()
