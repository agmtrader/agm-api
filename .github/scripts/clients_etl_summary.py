"""Format a terminal clients overview for Actions and email; no remote operations."""
from collections import defaultdict
import json
import os
from pathlib import Path
import uuid


def text(value):
    return 'unknown' if value is None else ' '.join(str(value).split())


def section(lines, title, details, empty='none'):
    lines.append(f'{title}:')
    lines.extend(f'  - {text(item)}' for item in (details or [empty]))


def failures(stage, name, key):
    details = []
    if stage.get('error') or stage.get('status') == 'failed':
        details.append(f'{name} stage: {text(stage.get("error") or "No error details returned")}')
    for item in stage.get(key, []):
        failed_files = item.get('failed_files', [])
        for file in failed_files:
            details.append(f'{item.get("name", name)} / {file.get("from") or file.get("file") or "unknown file"}: '
                           f'{text(file.get("error") or "No error details returned")}')
        if item.get('status') in ('failed', 'partial') and not failed_files:
            error = item.get('error')
            if not error and item.get('name') == 'account_details' and item.get('summary'):
                error = f'{item["summary"].get("failed", "unknown")} account failures (details below)'
            details.append(f'{item.get("name", "unknown")}: {error or "No error details returned"}')
    return details


def render(payload, execution):
    execution_status = execution.get('status', {})
    run_id = (payload.get('run_id') or execution.get('metadata', {}).get('name')
              or os.environ.get('CLIENTS_ETL_EXECUTION'))
    workflow = (f'{os.environ["GITHUB_SERVER_URL"]}/{os.environ["GITHUB_REPOSITORY"]}'
                f'/actions/runs/{os.environ["GITHUB_RUN_ID"]}')
    lines = [f'Workflow run: {workflow}',
             f'Completed (UTC): {text(execution_status.get("completionTime"))}',
             f'Clients ETL status: {text(payload.get("operational_status"))}',
             f'Execution: {text(run_id)}', f'Raw pipeline: {text(payload.get("status"))}']
    if run_id:
        lines.append(f'Full overview: gs://agm-datalake-etl-runs/runs/{text(run_id)}/overview.json')
    if not payload:
        lines.append('Clients job has no retrievable terminal overview. Review execution and workflow logs; '
                     'no API fallback or automatic retry was attempted.')
    stages = payload.get('overview', {})
    extract, backup, transform = (stages.get(name, {}) for name in ('extract', 'backup', 'transform'))
    for name, stage, keys in (
        ('Extract', extract, ('total', 'successful', 'skipped', 'failed')),
        ('Backup', backup, ()),
        ('Transform', transform, ('total', 'successful', 'failed')),
    ):
        counts = ', '.join(f'{key}: {text(stage.get("summary", {}).get(key))}' for key in keys)
        lines.append(f'{name} status: {text(stage.get("status"))}' + (f' ({counts})' if keys else ''))
        if name == 'Backup':
            steps = {s.get('name'): s for s in stage.get('steps', [])}
            for key, label, count in (
                ('rename_batch_files', 'Rename batch files', 'renamed'),
                ('sort_batch_files', 'Sort batch files', 'moved'),
                ('clear_batch_folder', 'Clear batch folder', None),
            ):
                step = steps.get(key, {})
                suffix = (f' ({count}: {text(step.get("summary", {}).get(count))}, '
                          f'failed: {text(step.get("summary", {}).get("failed"))})') if count else ''
                lines.append(f'  - {label}: {text(step.get("status"))}{suffix}')
        section(lines, f'{name} failures', failures(stage, name.lower(), 'reports' if name == 'Transform' else 'steps'),
                empty='none' if stage else 'unavailable')

    detail_steps = [s for s in extract.get('steps', []) if s.get('name') == 'account_details']
    for step in detail_steps:
        counts = step.get('summary', {})
        lines.append(f'IBKR refresh: {text(counts.get("successful"))} fresh, '
                     f'{text(counts.get("failed"))} failed, {text(counts.get("retained_stale"))} retained stale')
        if payload.get('operational_status') == 'completed_with_accepted_coverage_gaps':
            lines.append('Account coverage gaps match the accepted baseline; retained stale records are not current.')
        groups = defaultdict(list)
        for outcome in step.get('outcomes', []):
            if outcome.get('status') == 'failed':
                groups[text(outcome.get('error') or 'No error details returned')].append(text(outcome.get('account_id')))
        details = []
        for error, accounts in sorted(groups.items(), key=lambda pair: (-len(pair[1]), pair[0])):
            sample = ', '.join(accounts[:5])
            if len(accounts) > 5:
                sample += f'; {len(accounts) - 5} more (see full overview)'
            details.append(f'{error}: {len(accounts)} accounts (examples: {sample})')
        section(lines, 'IBKR account failures', details,
                empty='details unavailable; see full overview' if counts.get('failed') else 'none')

    warnings = []
    for warning in payload.get('warnings', []):
        warnings.append(f'{warning.get("name", "unknown")}: {warning.get("message", "No message returned")} '
                        f'Backup: {text(warning.get("backup_file"))}. Error: {text(warning.get("error"))}')
    section(lines, 'Report warnings', warnings)

    errors = [f'{key}: {text(payload[key])}' for key in
              ('error', 'error_type', 'error_code', 'error_id', 'overview_publication_error', 'lock_release_error')
              if payload.get(key) is not None]
    for condition in execution_status.get('conditions', []):
        if condition.get('type') == 'Completed' and condition.get('status') != 'True':
            errors.append(f'Cloud Run: {text(condition.get("reason"))}: {text(condition.get("message"))}')
    section(lines, 'Runner / execution errors', errors, empty='none' if payload else 'overview unavailable')
    return '\n'.join(lines)


def load(path):
    try:
        value = json.loads(path.read_text())
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


def main():
    temp = Path(os.environ['RUNNER_TEMP'])
    summary = render(load(temp / 'overview.json'), load(temp / 'execution.json'))
    (temp / 'clients-etl-summary.txt').write_text(summary + '\n')
    # A random delimiter prevents upstream multiline errors from truncating the output.
    delimiter = f'SUMMARY_{uuid.uuid4().hex}'
    with open(os.environ['GITHUB_OUTPUT'], 'a') as stream:
        stream.write(f'summary<<{delimiter}\n{summary}\n{delimiter}\n')
    with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as stream:
        stream.write('```text\n' + summary.replace('```', "'''") + '\n```\n')
    print(summary)


if __name__ == '__main__':
    main()
