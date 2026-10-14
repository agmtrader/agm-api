"""Provision clients job from verified current runtime; no Docker/build/API deployment."""
from pathlib import Path
import hashlib
import io
import json
import argparse
import zipfile
import google.auth
from google.auth.transport.requests import AuthorizedSession

ROOT=Path(__file__).resolve().parents[1]
PROJECT='agm-datalake'; REGION='us-central1'; BUCKET='agm-datalake-etl-runs'; JOB='agm-clients-etl'


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--staging-folder',required=True)
    parser.add_argument('--coverage-report',type=Path,required=True)
    parser.add_argument('--update',action='store_true')
    args=parser.parse_args()
    staging=args.staging_folder
    report=json.loads(args.coverage_report.read_text())
    accepted=[{'account_id':o['account_id'],'error':o['error']} for o in report['outcomes'] if o['status']=='failed']
    credentials,_=google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
    session=AuthorizedSession(credentials)
    service=session.get(f'https://run.googleapis.com/v2/projects/{PROJECT}/locations/{REGION}/services/agm-api',timeout=30)
    service.raise_for_status(); template=service.json()['template']; image=template['containers'][0]['image']
    buffer=io.BytesIO()
    files=['src/jobs/__init__.py','src/jobs/clients_etl.py','src/jobs/storage.py',
        'src/components/tools/private/etl.py','src/components/tools/private/ibkr_details_refresh.py']
    with zipfile.ZipFile(buffer,'w',zipfile.ZIP_DEFLATED) as archive:
        for file in files: archive.write(ROOT/file,file)
        archive.writestr('cache/accepted_coverage.json',json.dumps(accepted))
    content=buffer.getvalue();digest=hashlib.sha256(content).hexdigest();name=f'artifacts/clients-{digest}.zip'
    upload=session.post(f'https://storage.googleapis.com/upload/storage/v1/b/{BUCKET}/o',
        params={'uploadType':'media','name':name,'ifGenerationMatch':'0'},headers={'Content-Type':'application/zip'},data=content,timeout=60)
    if upload.status_code==412:
        from urllib.parse import quote
        upload=session.get(f'https://storage.googleapis.com/storage/v1/b/{BUCKET}/o/{quote(name,safe="")}',timeout=30)
    upload.raise_for_status();generation=upload.json()['generation']
    bootstrap=(ROOT/'src/jobs/bootstrap.py').read_text()
    vpc=dict(template['vpcAccess'])
    if not vpc['connector'].startswith('projects/'):
        vpc['connector']=f"projects/{PROJECT}/locations/{REGION}/connectors/{vpc['connector']}"
    env={'DEV_MODE':'false','ETL_BATCH_FOLDER_ID':staging,
        'CLIENTS_MANUAL_SOURCE_FOLDER_ID':'1N3LwrG7IossvCrrrFufWMb26VOcRxhi8',
        'ETL_STORAGE_BUCKET':BUCKET,'ETL_BUNDLE_NAME':name,'ETL_BUNDLE_GENERATION':generation,'ETL_BUNDLE_SHA256':digest}
    job={'labels':{'purpose':'clients-etl','managed-by':'codex'},
        'template':{'taskCount':1,'parallelism':1,'template':{
            'serviceAccount':template['serviceAccount'],'timeout':'3600s','maxRetries':0,
            'vpcAccess':vpc,
            'containers':[{'image':image,'command':['python'],'args':['-c',bootstrap],
                           'env':[{'name':k,'value':v} for k,v in env.items()],
                           'resources':{'limits':{'cpu':'1','memory':'2Gi'}}}]}}}
    Path('/private/tmp/agm-clients-job.json').write_text(json.dumps(job,indent=2))
    base=f'https://run.googleapis.com/v2/projects/{PROJECT}/locations/{REGION}/jobs'
    if args.update:
        create=session.patch(f'{base}/{JOB}',params={'updateMask':'template,labels'},json=job,timeout=60)
    else:
        create=session.post(base,params={'jobId':JOB},json=job,timeout=60)
    if not create.ok:
        raise RuntimeError(create.json().get('error', {}).get('message', f'HTTP {create.status_code}'))
    print(json.dumps({'operation':create.json()['name'],'job':JOB,'base_image':image,'bundle':name,'generation':generation,'sha256':digest,'accepted_gaps':len(accepted),'staging_folder':staging}))

if __name__=='__main__':main()
