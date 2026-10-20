"""Exercise the notification's actual workflow command without cloud calls or email."""
import json
import os
from pathlib import Path
import subprocess

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = yaml.safe_load((ROOT / '.github/workflows/etl_clients.yaml').read_text())
SCRIPT = next(s['run'] for s in WORKFLOW['jobs']['etl']['steps'] if s.get('id') == 'summary')


def overview():
    # Cloud Run envelope, with IBKR counts/status from the email and synthetic step data.
    return {
        'status': 'partial', 'operational_status': 'completed_with_accepted_coverage_gaps',
        'run_id': 'execution-fixture', 'warnings': [],
        'overview': {
            'extract': {'status': 'partial',
                        'summary': {'total': 12, 'successful': 10, 'skipped': 1, 'failed': 1},
                        'steps': [{'name': 'account_details', 'status': 'partial',
                                   'summary': {'total': 825, 'successful': 617, 'failed': 208,
                                               'retained_stale': 20},
                                   'outcomes': [{'account_id': f'SYNTHETIC{i}', 'status': 'failed',
                                                 'error': 'missing_master_account'} for i in range(208)]}]},
            'backup': {'status': 'success', 'steps': [
                {'name': 'rename_batch_files', 'status': 'success', 'summary': {'renamed': 8, 'failed': 0}},
                {'name': 'sort_batch_files', 'status': 'success', 'summary': {'moved': 8, 'failed': 0}},
                {'name': 'clear_batch_folder', 'status': 'success'}]},
            'transform': {'status': 'success', 'summary': {'total': 11, 'successful': 11, 'failed': 0},
                          'reports': []}}}


def run_summary(tmp_path, payload, execution=None, malformed=False):
    if payload is not None:
        (tmp_path / 'overview.json').write_text('{' if malformed else json.dumps(payload))
    if execution is not None:
        (tmp_path / 'execution.json').write_text(json.dumps(execution))
    env = dict(os.environ, RUNNER_TEMP=str(tmp_path), GITHUB_OUTPUT=str(tmp_path / 'output'),
               GITHUB_STEP_SUMMARY=str(tmp_path / 'step-summary'), GITHUB_SERVER_URL='https://github.com',
               GITHUB_REPOSITORY='agmtrader/agm-api', GITHUB_RUN_ID='12345')
    proc = subprocess.run(['bash', '-e', '-o', 'pipefail', '-c', SCRIPT], cwd=ROOT,
                          env=env, capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr
    output = (tmp_path / 'output').read_text()
    assert 'summary<<' in output
    return output, (tmp_path / 'step-summary').read_text() if (tmp_path / 'step-summary').exists() else ''


def test_accepted_gaps_include_counts_reasons_and_run_links(tmp_path):
    output, step_summary = run_summary(tmp_path, overview(), {
        'status': {'completionTime': '2026-10-16T10:15:00Z',
                   'conditions': [{'type': 'Completed', 'status': 'True'}]}})
    for expected in (
        'https://github.com/agmtrader/agm-api/actions/runs/12345',
        'Completed (UTC): 2026-10-16T10:15:00Z', 'Execution: execution-fixture',
        'completed_with_accepted_coverage_gaps', 'Raw pipeline: partial',
        'Extract status: partial (total: 12, successful: 10, skipped: 1, failed: 1)',
        'Rename batch files: success (renamed: 8, failed: 0)',
        'Sort batch files: success (moved: 8, failed: 0)', 'Clear batch folder: success',
        'Transform status: success (total: 11, successful: 11, failed: 0)',
        '617 fresh, 208 failed, 20 retained stale', 'missing_master_account: 208 accounts',
        'identifiers in private overview', 'Report warnings:\n  - none',
        'gs://agm-datalake-etl-runs/runs/execution-fixture/overview.json',
    ):
        assert expected in output
        assert expected in step_summary
    assert 'SYNTHETIC207' not in output  # Bounded samples; full outcomes stay in the overview.


def test_all_failure_levels_and_fallback_warning_are_preserved(tmp_path):
    payload = overview()
    payload['operational_status'] = 'failed'
    payload['overview']['extract']['steps'].append({
        'name': '794867', 'status': 'failed', 'error': 'Error Code: 1019, Statement generation in progress'})
    payload['warnings'] = [{'name': '794867', 'message': 'New file was not generated; existing backup was published.',
                            'backup_file': 'prior.csv', 'error': 'Error Code: 1019'}]
    payload['overview']['backup']['status'] = 'partial'
    payload['overview']['backup']['steps'][0].update(status='partial',
        failed_files=[{'from': 'clients.xls', 'error': 'rename denied'}])
    payload['overview']['backup']['steps'][1].update(status='partial',
        failed_files=[{'file': 'renamed.xls', 'error': 'move denied'}])
    payload['overview']['backup']['steps'][2].update(status='failed', error='clear denied')
    payload['overview']['transform'] = {'status': 'partial',
        'reports': [{'name': 'clients', 'status': 'failed', 'error': 'invalid columns'}]}
    output, _ = run_summary(tmp_path, payload)
    for expected in ('794867: IBKR 1019', 'clients.xls:', 'renamed.xls:',
                     'clear_batch_folder:', 'clients:', 'fallback warning', 'Backup: prior.csv',
                     'Failure details withheld'):
        assert expected in output


def test_stage_and_runner_errors_and_cloud_failure_survive(tmp_path):
    payload = {'status': 'failed', 'operational_status': 'failed', 'error_type': 'PermissionError',
               'error_code': 403, 'error_id': 'synthetic-error', 'lock_release_error': 'PreconditionFailed',
               'overview': {'extract': {'status': 'failed', 'error': 'source unavailable'}}}
    output, _ = run_summary(tmp_path, payload, {'status': {'conditions': [
        {'type': 'Completed', 'status': 'False', 'reason': 'NonZeroExitCode', 'message': 'Task exited 1'}]}})
    for expected in ('extract stage: Failure details withheld', 'PermissionError', '403',
                     'lock_release_error: PreconditionFailed', 'NonZeroExitCode', 'inspect private execution logs'):
        assert expected in output
    assert 'total: unknown' in output
    assert 'Transform failures:\n  - unavailable' in output


@pytest.mark.parametrize('malformed', [False, True])
def test_missing_or_invalid_overview_is_actionable(tmp_path, malformed):
    output, _ = run_summary(tmp_path, {} if malformed else None, {
        'metadata': {'name': 'execution-fixture'}, 'status': {'conditions': [
            {'type': 'Completed', 'status': 'False', 'reason': 'DeadlineExceeded'}]}}, malformed=malformed)
    assert 'https://github.com/agmtrader/agm-api/actions/runs/12345' in output
    assert 'no retrievable terminal overview' in output
    assert 'DeadlineExceeded' in output
    assert 'Execution: execution-fixture' in output


def test_clean_run_shows_explicit_empty_error_sections(tmp_path):
    payload = overview()
    payload.update(status='success', operational_status='success')
    payload['overview']['extract'].update(status='success', steps=[])
    output, _ = run_summary(tmp_path, payload)
    for stage in ('Extract', 'Backup', 'Transform'):
        assert f'{stage} failures:\n  - none' in output


def test_notification_does_not_export_raw_config_or_provider_credentials(tmp_path):
    payload = overview()
    marker = 'sensitive-marker-fixture'
    payload['overview']['transform'] = {'status': 'partial', 'reports': [
        {'name': 'nav', 'status': 'failed', 'error': "Error processing {'backup_folder_id': '" + marker + "', 'token': '" + marker + "'}"}]}
    payload['overview']['extract']['steps'][0]['outcomes'][0]['error'] = marker
    output, step_summary = run_summary(tmp_path, payload)
    assert marker not in output + step_summary
    assert 'Failure details withheld' in output
    assert 'nav:' in output
    assert 'https://github.com/agmtrader/agm-api/actions/runs/12345' in output
