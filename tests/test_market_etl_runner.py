"""Market migration: real runner/Flask/Actions boundaries; providers are in memory."""
import copy
import importlib.util
import json
import os
from pathlib import Path
import subprocess
from unittest.mock import Mock

from flask import Flask
from flask_jwt_extended import JWTManager, create_access_token
import pytest

from src.jobs import market_etl, market_dispatch
from src.jobs.market_result import EXPECTED_SOURCES, render_market
from src.components.tools.private import etl
from src.utils import authz
from src.app.tools.private.etl import bp

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('market_summary', ROOT / '.github/scripts/market_etl_summary.py')
summary = importlib.util.module_from_spec(spec)
spec.loader.exec_module(summary)
RUN = 'agm-market-etl-fixture'


def outcome():
    steps = [{'name': name, 'status': 'success', 'source_file_id': 'source-' + name} for name in EXPECTED_SOURCES]
    return {'status': 'success', 'pipeline': 'market_data', 'operational_status': 'success', 'run_id': RUN,
            'overview': {'extract': {'status': 'success', 'summary': {'total': 4, 'successful': 4, 'failed': 0, 'skipped': 0}, 'steps': steps},
                         'backup': {'status': 'success', 'steps': [{'name': 'clear_batch_folder', 'status': 'success'}]},
                         'transform': {'status': 'success', 'summary': {'total': 4, 'successful': 4, 'failed': 0}, 'reports': copy.deepcopy(steps)}}}


@pytest.fixture
def runner(monkeypatch):
    store = Mock()
    published = []
    def put(name, value):
        published.append((name, copy.deepcopy(value)))
        return 'owned-generation'
    store.put.side_effect = put
    monkeypatch.setattr(market_etl, 'RunStorage', lambda bucket: store)
    monkeypatch.setenv('ETL_BATCH_FOLDER_ID', 'isolated-market')
    monkeypatch.setenv('CLOUD_RUN_EXECUTION', RUN)
    monkeypatch.setattr(etl, 'batch_folder_id', 'isolated-market')
    monkeypatch.setattr(etl, 'Drive', Mock(get_files_in_folder=Mock(return_value=[])))
    monkeypatch.setattr('src.utils.connectors.supabase.initialize_database', lambda: None)
    return store, published


def run(tmp_path):
    return market_etl.main(['--output-dir', str(tmp_path), '--storage-bucket', 'synthetic-private'])


def test_actual_pipeline_runner_propagates_four_sources_and_saves_after_release(runner, monkeypatch, tmp_path):
    store, published = runner
    # Real run_pipeline/extract_data evaluate actual DataFrame return values and source IDs.
    import pandas as pd
    def extract(config):
        config['_extracted_source'] = {'id': 'source-' + config['name'], 'name': config['name'] + '.csv'}
        return pd.DataFrame([{'price': 101}])
    config = {'name': 'market_data', 'files': [{'name': name, 'pipeline': 'market_data', 'extract_func': extract} for name in EXPECTED_SOURCES]}
    monkeypatch.setattr(etl, '_get_etl_config_by_name', lambda name: copy.deepcopy(config))
    monkeypatch.setattr(etl, 'backup_data', lambda config: outcome()['overview']['backup'])
    def transform(config):
        reports = [{'name': f['name'], 'status': 'success', 'source_file_id': f['_extracted_source']['id']} for f in config['files']]
        return {'status': 'success', 'summary': {'total': 4, 'successful': 4, 'failed': 0}, 'reports': reports}
    monkeypatch.setattr(etl, 'transform_data', transform)
    assert run(tmp_path) == 0
    saved = published[-1][1]
    assert saved['operational_status'] == 'success'
    assert all(s['row_count'] == 1 for s in saved['overview']['extract']['steps'])
    assert (tmp_path / 'overview.json').stat().st_mode & 0o777 == 0o600
    assert store.mock_calls[-2][0] == 'delete'
    assert store.mock_calls[-1][0] == 'put'


@pytest.mark.parametrize('problem', ['lock', 'stage', 'shared', 'partial', 'provenance', 'release', 'publication'])
def test_failures_cannot_report_success_or_replay(runner, monkeypatch, tmp_path, problem):
    store, published = runner
    result = outcome()
    if problem == 'lock':
        original = store.put.side_effect
        store.put.side_effect = lambda name, value: (_ for _ in ()).throw(RuntimeError('412')) if name.startswith('locks/') else original(name, value)
    if problem == 'stage': etl.Drive.get_files_in_folder.return_value = [{'id': 'uncertain-old-file'}]
    if problem == 'shared': monkeypatch.setenv('ETL_BATCH_FOLDER_ID', market_etl.SHARED_BATCH)
    if problem == 'partial': result['overview']['extract']['status'] = 'partial'
    if problem == 'provenance': result['overview']['transform']['reports'][0]['source_file_id'] = 'stale-file'
    if problem == 'release': store.delete.side_effect = RuntimeError('release failed')
    if problem == 'publication':
        original = store.put.side_effect
        store.put.side_effect = lambda name, value: (_ for _ in ()).throw(RuntimeError('publication failed')) if name.startswith('runs/') else original(name, value)
    pipeline = Mock(return_value=result)
    monkeypatch.setattr(etl, 'run_pipeline', pipeline)
    assert run(tmp_path) == 1
    assert json.loads((tmp_path / 'overview.json').read_text())['operational_status'] == 'failed'
    assert pipeline.call_count == (0 if problem in ('lock', 'stage', 'shared') else 1)
    etl.Drive.clear_folder.assert_not_called()
    if problem == 'lock': store.delete.assert_not_called()
    if problem == 'release': assert published[-1][1]['lock_release_error'] == 'RuntimeError'


@pytest.mark.parametrize('bad', [None, {'status': 'success'}, {'status': 'success', 'overview': []}])
def test_missing_or_malformed_stage_results_fail_closed(bad):
    assert not render_market(bad, EXPECTED_SOURCES)[0]


def test_omitted_source_cannot_pass_with_self_consistent_counts():
    result = outcome()
    for name, key in [('extract', 'steps'), ('transform', 'reports')]:
        stage = result['overview'][name]
        stage[key].pop()
        stage['summary'].update(total=3, successful=3)
    assert not render_market(result, EXPECTED_SOURCES)[0]


def test_flask_dispatch_uses_real_jwt_scope_and_does_not_run_pipeline(monkeypatch):
    app = Flask(__name__)
    app.config['JWT_SECRET_KEY'] = 'synthetic-test-signing-key-at-least-32-bytes'
    JWTManager(app)
    users = {'allowed': {'id': 'allowed', 'scopes': 'etl/market_data'}, 'denied': {'id': 'denied', 'scopes': 'accounts/read'}}
    monkeypatch.setattr(authz, '_load_user', users.get)
    app.before_request(authz.authenticate_request)
    app.before_request(authz.enforce_route_scope)
    app.register_blueprint(bp, url_prefix='/etl')
    response = Mock(ok=True)
    response.json.return_value = {'name': market_dispatch.PARENT + '/operations/fixture-operation'}
    session = Mock(post=Mock(return_value=response))
    monkeypatch.setattr(market_dispatch, '_session', lambda: session)
    pipeline = Mock(side_effect=AssertionError('Flask must not execute ETL'))
    monkeypatch.setattr(etl, 'run_pipeline', pipeline)
    http = app.test_client()
    assert http.get('/etl/market_data').status_code == 401
    with app.app_context():
        denied, allowed = (create_access_token(identity=name) for name in ('denied', 'allowed'))
    assert http.get('/etl/market_data', headers={'Authorization': 'Bearer ' + denied}).status_code == 403
    r = http.post('/etl/market_data', headers={'Authorization': 'Bearer ' + allowed})
    assert r.status_code == 202 and r.json['operation'] == 'fixture-operation'
    assert r.headers['Cache-Control'] == 'no-store'
    assert session.post.call_count == 1
    pipeline.assert_not_called()
    execution = {'name': market_dispatch.JOB + '/executions/' + RUN,
                 'conditions': [{'type': 'Completed', 'state': 'CONDITION_SUCCEEDED'}]}
    response.json.return_value = {'done': True, 'response': execution}
    session.get.return_value = response
    r = http.get('/etl/market_data?operation=fixture-operation', headers={'Authorization': 'Bearer ' + allowed})
    assert r.status_code == 200 and r.json['status'] == 'completed'
    assert session.post.call_count == 1
    execution['conditions'][0]['state'] = 'CONDITION_FAILED'
    assert http.get('/etl/market_data?operation=fixture-operation', headers={'Authorization': 'Bearer ' + allowed}).json['status'] == 'failed'
    response.json.return_value = {'done': True, 'response': {'name': market_dispatch.PARENT + '/jobs/other/executions/other'}}
    assert http.get('/etl/market_data?operation=fixture-operation', headers={'Authorization': 'Bearer ' + allowed}).status_code == 404


def test_uncertain_dispatch_has_no_retry(monkeypatch):
    import requests
    session = Mock()
    session.post.side_effect = requests.Timeout('secret provider payload')
    monkeypatch.setattr(market_dispatch, '_session', lambda: session)
    from src.utils.exception import ServiceError
    with pytest.raises(ServiceError) as exc: market_dispatch.dispatch()
    assert exc.value.code == 'market_job_outcome_unknown'
    assert session.post.call_count == 1


@pytest.mark.parametrize('problem', ['', 'cloud', 'identity', 'raw', 'source', 'lock', 'overview'])
def test_actual_actions_shell_waits_for_exact_execution_and_rejects_false_success(tmp_path, problem):
    import yaml
    workflow = yaml.safe_load((ROOT / '.github/workflows/etl_market_data.yaml').read_text())
    steps = workflow['jobs']['etl']['steps']
    script = next(s['run'] for s in steps if s.get('id') == 'execute')
    terminal = next(s['run'] for s in steps if s.get('id') == 'terminal-summary')
    payload = outcome()
    if problem == 'identity': payload['run_id'] = 'agm-market-etl-other'
    if problem == 'raw': payload['overview']['extract']['status'] = 'partial'
    if problem == 'source': payload['overview']['transform']['reports'][0]['source_file_id'] = 'stale'
    if problem == 'lock': payload['lock_release_error'] = 'secret must not leak'
    if problem == 'overview': payload = {}
    fixture = tmp_path / 'fixture.json'; fixture.write_text(json.dumps(payload))
    binary = tmp_path / 'gcloud'
    binary.write_text('''#!/usr/bin/env python3
import json,os,sys,shutil
args=sys.argv[1:]
with open(os.environ['CALLS'],'a') as f: f.write(json.dumps(args)+'\\n')
if args[:4]==['run','jobs','execute','agm-market-etl']: print('agm-market-etl-fixture')
elif args[:4]==['run','jobs','executions','describe']:
 assert args[4]=='agm-market-etl-fixture'
 print(json.dumps({'metadata':{'name':'agm-market-etl-fixture'},'status':{'completionTime':'2026-10-20T00:00:00Z','conditions':[{'type':'Completed','status':os.environ['COMPLETED']}]}}))
elif args[:2]==['storage','cp']:
 assert args[2].endswith('/runs/agm-market-etl-fixture/overview.json')
 shutil.copy(os.environ['FIXTURE'],args[3])
else: raise SystemExit(2)
''')
    binary.chmod(0o700)
    env = dict(os.environ, PATH=str(tmp_path) + os.pathsep + os.environ['PATH'], RUNNER_TEMP=str(tmp_path),
               GITHUB_OUTPUT=str(tmp_path / 'output'), GITHUB_STEP_SUMMARY=str(tmp_path / 'summary'),
               MARKET_ETL_EXECUTION=RUN, CALLS=str(tmp_path / 'calls'), FIXTURE=str(fixture), COMPLETED='False' if problem == 'cloud' else 'True')
    proc = subprocess.run(['bash', '-e', '-o', 'pipefail', '-c', script], cwd=ROOT, env=env, capture_output=True, text=True)
    assert (proc.returncode == 0) == (not problem), proc.stderr
    assert subprocess.run(['bash', '-e', '-o', 'pipefail', '-c', terminal], cwd=ROOT, env=env, capture_output=True).returncode == 0
    calls = [json.loads(line) for line in (tmp_path / 'calls').read_text().splitlines()]
    assert sum(args[:3] == ['run', 'jobs', 'execute'] for args in calls) == 1
    output = (tmp_path / 'output').read_text()
    assert 'secret must not leak' not in output
    assert f'Verified terminal success: {not problem}' in output


def test_deployment_artifact_has_exact_bootstrap_allowlist_and_preserves_runtime():
    import io
    import zipfile
    from src.jobs import market_bootstrap
    spec = importlib.util.spec_from_file_location('deploy_market', ROOT / 'dev/deploy_market_job.py')
    deploy = importlib.util.module_from_spec(spec); spec.loader.exec_module(deploy)
    with zipfile.ZipFile(io.BytesIO(deploy.bundle())) as archive:
        assert set(archive.namelist()) == market_bootstrap.ALLOWED
        assert all(archive.read(name) == (ROOT / name).read_bytes() for name in archive.namelist())
    template = {'serviceAccount': 'synthetic-runtime', 'vpcAccess': {'connector': 'synthetic-vpc', 'egress': 'ALL_TRAFFIC'},
                'containers': [{'image': 'registry/api@sha256:' + 'a' * 64}]}
    result = deploy.configuration(template, 'isolated-market', 'artifact.zip', '123', 'digest')
    task = result['template']['template']
    assert task['maxRetries'] == 0 and task['serviceAccount'] == 'synthetic-runtime'
    assert task['containers'][0]['image'] == template['containers'][0]['image']
    assert template['vpcAccess']['connector'] == 'synthetic-vpc'
    for stage in (market_etl.SHARED_BATCH, market_etl.CLIENTS_BATCH):
        with pytest.raises(ValueError): deploy.configuration(template, stage, 'artifact.zip', '123', 'digest')
