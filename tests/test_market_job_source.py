"""Source deployment preserves the existing job and rejects active/uncertain runs."""
import copy
import io
from pathlib import Path
from urllib.error import HTTPError
import zipfile

import pytest
from dev import sync_market_job as sync
from src.jobs.market_bootstrap import ALLOWED


class Cloud:
    def __init__(self, active=False, locked=False):
        self.active, self.locked = active, locked
        self.calls, self.content = [], None
        self.job = {'name': sync.PARENT + '/jobs/' + sync.JOB, 'etag': 'current-etag',
                    'labels': {'purpose': 'market-etl'}, 'template': {'taskCount': 1,
                    'template': {'serviceAccount': 'existing-account', 'maxRetries': 0,
                    'timeout': '3600s', 'vpcAccess': {'connector': 'existing-connector'},
                    'containers': [{'image': 'registry/api@sha256:' + 'a' * 64,
                    'command': ['python'], 'args': ['old'], 'resources': {'limits': {'cpu': '1', 'memory': '2Gi'}},
                    'env': [{'name': key, 'value': value} for key, value in {
                        'ETL_STORAGE_BUCKET': 'existing-bucket', 'ETL_BATCH_FOLDER_ID': 'existing-market-stage',
                        'ETL_BUNDLE_NAME': 'old.zip', 'ETL_BUNDLE_GENERATION': '42', 'ETL_BUNDLE_SHA256': 'old'
                    }.items()] + [{'name': 'UNCHANGED_SECRET', 'valueSource': {'secretKeyRef': {'secret': 'fixture', 'version': '1'}}}]}]}}}

    def request(self, url, **kwargs):
        self.calls.append((url, kwargs))
        if 'locks%2Fmarket-etl.json' in url:
            if self.locked:
                return b'{}'
            raise HTTPError(url, 404, 'Not Found', {}, None)
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
            assert 'updateMask' not in url
            self.job.update(copy.deepcopy(kwargs['body']))
            return {'done': True}
        if '/executions?' in url:
            return {'executions': [{}] if self.active else [{'completionTime': '2026-10-22T00:00:00Z'}]}
        return copy.deepcopy(self.job)


def source(path):
    return b'committed: ' + path.encode()


def test_pinned_source_deploy_preserves_every_other_job_setting_and_never_dispatches():
    cloud = Cloud()
    before = copy.deepcopy(cloud.job['template'])
    assert sync.synchronize(cloud, source, 'b' * 40, apply=True)['verified']
    after = copy.deepcopy(cloud.job['template'])
    container = after['template']['containers'][0]
    assert container['args'] == ['-c', source('src/jobs/market_bootstrap.py').decode()]
    with zipfile.ZipFile(io.BytesIO(cloud.content)) as archive:
        assert set(archive.namelist()) == ALLOWED
        assert all(archive.read(path) == source(path) for path in ALLOWED)
    container['args'] = before['template']['containers'][0]['args']
    for old, new in zip(before['template']['containers'][0]['env'], container['env']):
        if old['name'].startswith('ETL_BUNDLE_'):
            new['value'] = old['value']
    assert after == before
    assert not any(':run' in url or ':execute' in url for url, _ in cloud.calls)


@pytest.mark.parametrize('active,locked', [(True, False), (False, True)])
def test_active_execution_or_lock_rejects_deployment_before_any_write(active, locked):
    cloud = Cloud(active, locked)
    with pytest.raises(ValueError):
        sync.synchronize(cloud, source, 'b' * 40, apply=True)
    assert all(kwargs.get('method', 'GET') == 'GET' for _, kwargs in cloud.calls)


def test_plan_does_not_mutate_and_actions_synchronizes_before_exact_execution():
    cloud = Cloud()
    assert sync.synchronize(cloud, source, 'b' * 40)['mode'] == 'plan'
    assert all(kwargs.get('method', 'GET') == 'GET' for _, kwargs in cloud.calls)
    text = (Path(__file__).parents[1] / '.github/workflows/etl_market_data.yaml').read_text()
    assert text.index('sync_market_job.py') < text.index('gcloud run jobs execute')
