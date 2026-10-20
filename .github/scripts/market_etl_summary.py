"""Validate exact terminal market execution and format a bounded safe summary."""
import argparse
import json
import os
from pathlib import Path
import re
import sys
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.jobs.market_result import EXPECTED_SOURCES, render_market


def load(path):
    try:
        data = json.loads(path.read_text())
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def render(payload, execution, run_id):
    valid_id = bool(re.fullmatch(r'agm-market-etl-[a-z0-9-]{1,80}', run_id or ''))
    metadata = execution.get('metadata') or {}
    state = execution.get('status') or {}
    good, summary = render_market(payload, EXPECTED_SOURCES)
    good = (good and valid_id and metadata.get('name') == run_id
            and payload.get('run_id') == run_id and payload.get('pipeline') == 'market_data'
            and payload.get('operational_status') == 'success'
            and not any(payload.get(key) for key in ('error_type', 'lock_release_error', 'overview_publication_error'))
            and bool(state.get('completionTime'))
            and any(c.get('type') == 'Completed' and c.get('status') == 'True' for c in state.get('conditions', [])))
    # Only fixed outcomes, counts and platform-generated identifiers leave private evidence.
    completed = state.get('completionTime', '')
    if not isinstance(completed, str) or not re.fullmatch(r'[0-9TZ:.+-]{1,40}', completed):
        completed = 'unavailable'
    lines = [f'Execution: {run_id if valid_id else "unavailable"}',
             f'Completed (UTC): {completed}',
             f'Verified terminal success: {bool(good)}', summary]
    workflow = f'{os.getenv("GITHUB_SERVER_URL", "https://github.com")}/{os.getenv("GITHUB_REPOSITORY", "agmtrader/agm-api")}/actions/runs/{os.getenv("GITHUB_RUN_ID", "unknown")}'
    lines.append(f'Workflow: {workflow}')
    if valid_id:
        lines.append(f'Private overview: gs://agm-datalake-etl-runs/runs/{run_id}/overview.json')
    if not payload:
        lines.append('Terminal overview unavailable; counts unavailable.')
    if any(payload.get(k) for k in ('error_type', 'lock_release_error', 'overview_publication_error')):
        lines.append('Runner, lock or publication failure; inspect private evidence.')
    return bool(good), '\n'.join(lines)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--validate', action='store_true')
    args = parser.parse_args()
    temp = Path(os.environ['RUNNER_TEMP'])
    good, summary = render(load(temp / 'overview.json'), load(temp / 'execution.json'), os.getenv('MARKET_ETL_EXECUTION', ''))
    if args.validate:
        return 0 if good else 1
    (temp / 'market-etl-summary.txt').write_text(summary + '\n')
    delimiter = 'SUMMARY_' + uuid.uuid4().hex
    with open(os.environ['GITHUB_OUTPUT'], 'a') as stream:
        stream.write(f'summary<<{delimiter}\n{summary}\n{delimiter}\n')
    with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as stream:
        stream.write(summary + '\n')
    print(summary)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
