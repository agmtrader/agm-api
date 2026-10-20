"""Run actual Actions shell commands against local fake HTTP responses."""
import json
import os
from pathlib import Path
import subprocess

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]


def screening_result():
    return {'status': 'success', 'screenings_skipped': False, 'screening_errors': [], 'screening_errors_count': 0,
            'total_screenings_planned': 859, 'screenings_executed': 859, 'contacts_targeted': 859,
            'contacts_with_no_screenings': 3, 'accounts_targeted': 500, 'accounts_with_no_screenings_at_all': 1,
            'accounts_with_some_contacts_no_screenings': 2, 'screenings_with_list_matches': 2,
            'screenings_without_list_matches': 857, 'ofac_matches': 1, 'uk_matches': 1, 'un_matches': 0}


def run_workflow(tmp_path, mode, body, status=200, transport=False):
    file, job = 'daily_screening.yaml', 'run-screenings'
    workflow = yaml.safe_load((ROOT / '.github/workflows' / file).read_text())
    steps = workflow['jobs'][job]['steps']
    operation = next(s['run'] for s in steps if s.get('id') == 'operation')
    terminal = next(s['run'] for s in steps if s.get('id') == 'terminal-summary')
    bindir = tmp_path / 'bin'
    bindir.mkdir()
    curl = bindir / 'curl'
    curl.write_text('#!/usr/bin/env python3\n'
        'import os, sys\n'
        'with open(os.environ["CALLS"], "a") as f: f.write(sys.argv[-1]+"\\n")\n'
        'if sys.argv[-1].endswith("/token"): print(\'{"access_token":"synthetic"}\\n200\')\n'
        'elif os.environ["TRANSPORT"] == "1": sys.exit(28)\n'
        'else: print(os.environ["BODY"] + "\\n" + os.environ["STATUS"])\n')
    curl.chmod(0o755)
    env = dict(os.environ, PATH=str(bindir) + os.pathsep + os.environ['PATH'],
        RUNNER_TEMP=str(tmp_path), GITHUB_OUTPUT=str(tmp_path / 'output'), GITHUB_STEP_SUMMARY=str(tmp_path / 'summary'),
        CALLS=str(tmp_path / 'calls'), AGM_API_EMAIL='synthetic@example.test', AGM_API_PASSWORD='synthetic',
        BODY=json.dumps(body), STATUS=str(status), TRANSPORT='1' if transport else '0')
    proc = subprocess.run(['bash', '-e', '-o', 'pipefail', '-c', operation], cwd=ROOT, env=env, capture_output=True, text=True)
    publish = subprocess.run(['bash', '-e', '-o', 'pipefail', '-c', terminal], cwd=ROOT, env=env, capture_output=True, text=True)
    assert publish.returncode == 0, publish.stderr
    assert len((tmp_path / 'calls').read_text().splitlines()) == 2  # Never replay a write-producing operation.
    return proc, (tmp_path / 'output').read_text()


def test_successful_workflow_validates_actual_counts(tmp_path):
    proc, output = run_workflow(tmp_path, 'screenings', screening_result())
    assert proc.returncode == 0, proc.stderr
    assert 'summary<<SUMMARY_' in output
    assert 'screenings_executed: 859' in output
    assert 'screenings_with_list_matches: 2' in output


# Market execution/false-success cases now exercise the actual Cloud Run shell
# in test_market_etl_runner.py, instead of the retired HTTP workflow.

@pytest.mark.parametrize('failure', ['error', 'count-mismatch', 'missing-metrics'])
def test_screening_errors_or_unknown_metrics_fail_even_with_http_200(tmp_path, failure):
    body = screening_result()
    if failure == 'error':
        body['screening_errors'] = ['synthetic failure']
        body['screening_errors_count'] = 1
    if failure == 'count-mismatch':
        body['screenings_without_list_matches'] = 0
    if failure == 'missing-metrics':
        del body['screenings_with_list_matches']
    proc, output = run_workflow(tmp_path, 'screenings', body)
    assert proc.returncode != 0
    if failure == 'missing-metrics':
        assert 'screenings_with_list_matches: unavailable' in output
