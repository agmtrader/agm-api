"""Run the actual workflow shell and renderer against fake HTTP responses."""
import json
import os
from pathlib import Path
import subprocess

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = yaml.safe_load((ROOT / '.github/workflows/send_unfunded_emails.yml').read_text())
STEPS = WORKFLOW['jobs']['send-unfunded-emails']['steps']


def result(statuses):
    recipients = [{'status': status, 'name': '<Example>|\nClient', 'email': 'example@example.test',
                   'ibkr_account_number': f'SYNTHETIC{i}',
                   'reason': 'Verify delivery before retrying' if status == 'failed' else ''}
                  for i, status in enumerate(statuses)]
    counts = {'total': len(statuses), **{s: statuses.count(s) for s in ('sent', 'failed', 'skipped')}}
    status = ('partial' if counts['sent'] else 'failed') if counts['failed'] else 'success'
    return {'status': status, 'summary': counts, 'recipients': recipients}


def run_workflow(tmp_path, payload, *, http_status='200', token_status='200', transport=False,
                 credentials=True, token_payload=None):
    bin_dir = tmp_path / 'bin'
    bin_dir.mkdir()
    curl = bin_dir / 'curl'
    curl.write_text('#!/usr/bin/env python3\n'
                    'import json, os, sys\n'
                    'from pathlib import Path\n'
                    'with open(os.environ["CALLS"], "a") as f: f.write(sys.argv[-1] + "\\n")\n'
                    'if sys.argv[-1].endswith("/token"):\n'
                    ' print(os.environ["TOKEN_BODY"] + "\\n" + os.environ["TOKEN_STATUS"])\n'
                    'elif os.environ["TRANSPORT"] == "1": sys.exit(28)\n'
                    'else: print(os.environ["BATCH_BODY"] + "\\n" + os.environ["BATCH_STATUS"])\n')
    curl.chmod(0o755)
    env = dict(os.environ, PATH=str(bin_dir) + os.pathsep + os.environ['PATH'],
               RUNNER_TEMP=str(tmp_path), GITHUB_STEP_SUMMARY=str(tmp_path / 'summary.md'),
               GITHUB_OUTPUT=str(tmp_path / 'outputs'), CALLS=str(tmp_path / 'calls'),
               AGM_API_EMAIL='synthetic@example.test' if credentials else '', AGM_API_PASSWORD='synthetic',
               TOKEN_BODY=json.dumps(token_payload if token_payload is not None else {'access_token': 'synthetic-token'}),
               TOKEN_STATUS=token_status, BATCH_BODY=json.dumps(payload), BATCH_STATUS=http_status,
               TRANSPORT='1' if transport else '0')
    completed = subprocess.run(['bash', '-e', '-o', 'pipefail', '-c', STEPS[1]['run']], cwd=ROOT,
                               env=env, capture_output=True, text=True)
    publish = subprocess.run(['bash', '-e', '-o', 'pipefail', '-c', STEPS[2]['run']], cwd=ROOT,
                             env=env, capture_output=True, text=True)
    assert publish.returncode == 0, publish.stderr
    return completed, (tmp_path / 'summary.md').read_text(), (tmp_path / 'outputs').read_text(), \
        (tmp_path / 'calls').read_text().splitlines() if (tmp_path / 'calls').exists() else []


@pytest.mark.parametrize('statuses,expected,exit_code', [
    (['sent', 'failed', 'sent', 'skipped'], 'partial', 0),
    (['sent', 'sent'], 'success', 0),
    (['failed', 'failed'], 'failed', 1),
    ([], 'success', 0),
    (['skipped'], 'success', 0),
])
def test_actual_outcomes_in_final_summary(tmp_path, statuses, expected, exit_code):
    completed, markdown, output, calls = run_workflow(tmp_path, result(statuses))
    assert completed.returncode == exit_code, completed.stderr
    assert f'status={expected}' in output
    assert f'| Total records | {len(statuses)} |' in markdown
    for key in ('sent', 'failed', 'skipped'):
        assert f'Total {key}: {statuses.count(key)}' in output
        assert f'| Total {key} | {statuses.count(key)} |' in markdown
    assert len(calls) == 2  # A single token and batch call, never a batch replay.
    if statuses:
        assert '&lt;Example&gt;&#124; Client' in markdown
    if 'failed' in statuses:
        assert 'Verify delivery before retrying' in output


def test_legacy_api_supports_workflow_first_rollout(tmp_path):
    completed, markdown, output, _ = run_workflow(tmp_path, [
        {'email': 'sent@example.test'}, {'email': None},
    ])
    assert completed.returncode == 0
    assert 'Legacy API response' in markdown
    assert 'Total sent: 1' in output
    assert 'Total skipped: 1' in output


@pytest.mark.parametrize('kwargs,call_count', [
    ({'http_status': '500'}, 2),
    ({'transport': True}, 2),
    ({'token_status': '401'}, 1),
    ({'token_payload': {}}, 1),
    ({'credentials': False}, 0),
])
def test_fatal_runs_publish_unknown_counts_without_retry(tmp_path, kwargs, call_count):
    completed, markdown, output, calls = run_workflow(tmp_path, {'error_id': 'synthetic-error'}, **kwargs)
    assert completed.returncode == 1
    assert 'status=failed' in output
    assert '| Total sent | Unknown |' in markdown
    assert '| Total failed | Unknown |' in markdown
    assert 'before retrying' in output
    assert len(calls) == call_count


def test_inconsistent_api_counts_are_not_reported_as_success(tmp_path):
    payload = result(['failed'])
    payload['summary']['sent'] = 10
    completed, _, output, _ = run_workflow(tmp_path, payload)
    assert completed.returncode == 1
    assert 'disagree' in output
    assert 'counts are unknown' in output
