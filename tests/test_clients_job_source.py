import copy
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import zipfile

import pytest

spec = importlib.util.spec_from_file_location('sync_clients_job', Path(__file__).parents[1] / 'dev/sync_clients_job.py')
sync = importlib.util.module_from_spec(spec)
spec.loader.exec_module(sync)


def old_bundle():
    output = io.BytesIO()
    with zipfile.ZipFile(output, 'w') as archive:
        for name in sync.FILES:
            archive.writestr(name, b'old deployed source')
        archive.writestr(sync.BASELINE, b'[{"account_id":"accepted-fixture","error":"known_gap"}]\n')
    return output.getvalue()


class Cloud:
    def __init__(self):
        self.old = old_bundle()
        self.calls = []
        self.content = None
        self.job = {'name': sync.PARENT + '/jobs/' + sync.JOB, 'etag': 'current-etag',
                    'labels': {'purpose': 'clients-etl'}, 'template': {'taskCount': 1,
                    'template': {'serviceAccount': 'existing-account', 'maxRetries': 0,
                    'timeout': '3600s', 'vpcAccess': {'connector': 'existing-connector'},
                    'containers': [{'image': 'old-image', 'command': ['python'], 'args': ['old'],
                    'resources': {'limits': {'cpu': '1', 'memory': '2Gi'}}, 'env': [
                        {'name': 'ETL_STORAGE_BUCKET', 'value': 'existing-bucket'},
                        {'name': 'ETL_BATCH_FOLDER_ID', 'value': 'existing-clients-stage'},
                        {'name': 'ETL_BUNDLE_NAME', 'value': 'old.zip'},
                        {'name': 'ETL_BUNDLE_GENERATION', 'value': '42'},
                        {'name': 'ETL_BUNDLE_SHA256', 'value': hashlib.sha256(self.old).hexdigest()},
                        {'name': 'UNCHANGED_SECRET', 'valueSource': {'secretKeyRef': {'secret': 'fixture', 'version': '1'}}}]}]}}}

    def request(self, url, **kwargs):
        self.calls.append((url, kwargs))
        if 'old.zip?alt=media&generation=42' in url:
            return self.old
        assert '?alt=media&generation=43' in url
        return self.content

    def json(self, url, **kwargs):
        self.calls.append((url, kwargs))
        if kwargs.get('method') == 'POST':
            assert 'ifGenerationMatch=0' in url
            self.content = kwargs['body']
            return {'generation': '43'}
        if kwargs.get('method') == 'PATCH':
            assert kwargs['body']['etag'] == 'current-etag'
            self.job.update(copy.deepcopy(kwargs['body']))
            return {'done': True}
        if url.endswith('/services/agm-api'):
            return {'trafficStatuses': [{'percent': 100, 'revision': 'serving-revision'}]}
        if url.endswith('/revisions/serving-revision'):
            return {'containers': [{'image': 'registry/api@sha256:' + 'a' * 64}]}
        return copy.deepcopy(self.job)


def source(path):
    return b'new committed source: ' + path.encode()


def test_refresh_replaces_stale_source_and_preserves_baseline_and_runtime_policy():
    cloud = Cloud()
    before = copy.deepcopy(cloud.job['template'])
    result = sync.synchronize(cloud, source, 'b' * 40, apply=True)
    assert result['verified']
    with zipfile.ZipFile(io.BytesIO(cloud.content)) as archive:
        for path in sync.FILES:
            assert archive.read(path) == source(path)
        with zipfile.ZipFile(io.BytesIO(cloud.old)) as previous:
            assert archive.read(sync.BASELINE) == previous.read(sync.BASELINE)
    after = cloud.job['template']
    new_container = after['template']['containers'][0]
    old_container = before['template']['containers'][0]
    assert new_container['args'] == ['-c', source('src/jobs/bootstrap.py').decode()]
    assert new_container['image'].endswith('a' * 64)
    for key in ('serviceAccount', 'maxRetries', 'timeout', 'vpcAccess'):
        assert after['template'][key] == before['template'][key]
    for old in old_container['env']:
        if not old['name'].startswith('ETL_BUNDLE_'):
            assert old in new_container['env']
    assert old_container['resources'] == new_container['resources']
    assert not any(':run' in url or ':execute' in url for url, _ in cloud.calls)


def test_plan_performs_no_cloud_writes_and_bundle_is_reproducible():
    cloud = Cloud()
    result = sync.synchronize(cloud, source, 'b' * 40)
    assert result['mode'] == 'plan'
    assert all(kwargs.get('method', 'GET') == 'GET' for _, kwargs in cloud.calls)
    assert sync.bundle(source, cloud.old, hashlib.sha256(cloud.old).hexdigest()) == sync.bundle(source, cloud.old, hashlib.sha256(cloud.old).hexdigest())


def test_corrupt_existing_artifact_stops_before_writes():
    cloud = Cloud()
    cloud.old += b'corrupted'
    with pytest.raises(ValueError, match='checksum mismatch'):
        sync.synchronize(cloud, source, 'b' * 40, apply=True)
    assert all(kwargs.get('method', 'GET') == 'GET' for _, kwargs in cloud.calls)


def test_workflows_gate_dispatch_and_refresh_after_successful_deploy():
    root = Path(__file__).parents[1]
    workflow = (root / '.github/workflows/etl_clients.yaml').read_text()
    assert workflow.index('sync_clients_job.py') < workflow.index('gcloud run jobs execute')
    assert 'continue-on-error' not in workflow
    deploy = (root / '.github/workflows/cloudbuild.yaml').read_text()
    assert deploy.index('gcloud builds submit') < deploy.index('sync_clients_job.py')
    assert 'github.event.workflow_run.head_sha || github.sha' in deploy
