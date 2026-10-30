"""Daily account-scoped task snapshots. No signatures or task completion calls."""
import csv
import io
import json
import re
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests

from src.components.tools.private.ibkr_details_refresh import Pacer
from src.utils.connectors.ibkr_web_api import IBKRWebAPI


SOURCES = {'pending': 'pendingTasks', 'registration': 'registrationTasks'}
COLUMNS = (
    'schema_version', 'source', 'record_type', 'account_id', 'account_title',
    'master_account', 'credential_master', 'snapshot_at', 'last_attempt_at',
    'last_success_at', 'refresh_status', 'error', 'http_status', 'attempts_json',
    'task_count', 'task_index', 'task_number', 'form_number', 'form_name',
    'entity_id', 'external_id', 'state', 'action', 'is_completed', 'date_completed',
    'required_for_approval', 'required_for_trading', 'online_task',
    'task_json', 'account_metadata_json',
)
ACCOUNT_FIELDS = tuple(field for field in COLUMNS[:15] if field != 'record_type')


def _text(value):
    return '' if value is None else str(value).strip()


def _json(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'), allow_nan=False)


def _safe(value):
    if isinstance(value, dict):
        return {key: '[REDACTED]' if re.search(r'token|password|secret|private.?key|authorization|url$|href$', key, re.I)
                else _safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_safe(item) for item in value]
    if isinstance(value, str) and re.search(r'(?:access_)?token=|Bearer\s+|eyJ[\w-]+\.[\w-]+\.', value, re.I):
        return '[REDACTED]'
    return value


def encode_snapshot(rows):
    stream = io.StringIO(newline='')
    writer = csv.DictWriter(stream, fieldnames=COLUMNS, lineterminator='\n')
    writer.writeheader()
    writer.writerows(rows)
    return stream.getvalue().encode('utf-8')


def decode_snapshot(raw, source):
    if source not in SOURCES:
        raise ValueError('Unsupported task source')
    reader = csv.DictReader(io.StringIO(raw.decode('utf-8-sig')), strict=True)
    if reader.fieldnames != list(COLUMNS):
        raise ValueError('Invalid task snapshot schema')
    rows = list(reader)
    coverage, tasks = {}, {}
    for row in rows:
        if None in row or any(value is None for value in row.values()):
            raise ValueError('Incomplete task snapshot row')
        key = row['account_id']
        valid_account = re.fullmatch(r'[A-Z]+\d+', key) or (key and row['error'] == 'invalid_account_number' and row['refresh_status'] == 'unavailable')
        if row['schema_version'] != '1' or row['source'] != source or not valid_account:
            raise ValueError('Task snapshot account/source mismatch')
        if row['refresh_status'] not in ('fresh', 'stale', 'unavailable'):
            raise ValueError('Invalid task snapshot freshness')
        if row['record_type'] == 'account':
            if key in coverage:
                raise ValueError('Duplicate account coverage in task snapshot')
            coverage[key] = row
            if not isinstance(json.loads(row['account_metadata_json']), dict) or not isinstance(json.loads(row['attempts_json']), list):
                raise ValueError('Invalid task snapshot account metadata')
        elif row['record_type'] == 'task':
            if not isinstance(json.loads(row['task_json']), dict):
                raise ValueError('Invalid task snapshot task')
            tasks.setdefault(key, []).append(row)
        else:
            raise ValueError('Invalid task snapshot record type')
    for key in set(coverage) | set(tasks):
        account = coverage.get(key)
        if account is None:
            raise ValueError('Task snapshot is missing account coverage')
        count = account['task_count']
        if count == '' and (account['last_success_at'] or tasks.get(key)):
            raise ValueError('Unknown task count with successful history')
        if count != '' and (not count.isdigit() or int(count) != len(tasks.get(key, []))):
            raise ValueError('Task snapshot count mismatch')
        indices = [row['task_index'] for row in tasks.get(key, [])]
        if indices != [str(index) for index in range(len(indices))]:
            raise ValueError('Missing or duplicated task snapshot positions')
        for row in tasks.get(key, []):
            if any(row[field] != account[field] for field in ACCOUNT_FIELDS):
                raise ValueError('Task snapshot coverage disagrees with task rows')
    return rows


def refresh_tasks(accounts, previous, source, *, workers=4, rate=4,
                  client_factory=IBKRWebAPI, get=requests.get, progress=None):
    """Keep a coverage row even for confirmed empty, stale and unreadable accounts.

    Credential selection matches the details refresher: only configured masters;
    one recorded alternate credential read on HTTP 403. No transport retries.
    """
    if source not in SOURCES or not 1 <= workers <= 10 or not 0 < rate <= 8:
        raise ValueError('Invalid task refresh configuration')
    previous = decode_snapshot(encode_snapshot(previous), source) if previous else []
    started = time.monotonic()
    stamp = datetime.now(timezone.utc).isoformat()
    prior_accounts = {row['account_id']: row for row in previous if row['record_type'] == 'account'}
    prior_tasks = {}
    for row in previous:
        if row['record_type'] == 'task':
            prior_tasks.setdefault(row['account_id'], []).append(row)
    candidates, titles = {}, {}
    for account in accounts:
        key = _text(account.get('ibkr_account_number')).upper()
        if key:
            candidates.setdefault(key, set()).add(_text(account.get('master_account')).upper())
            titles[key] = _text(account.get('title'))
    for key, row in prior_accounts.items():
        candidates.setdefault(key, {row['master_account']})
        titles.setdefault(key, row['account_title'])
    if not candidates:
        raise ValueError('Account population empty; previous task resource preserved')
    pacer, lock, clients = Pacer(rate), threading.Lock(), {}
    output, outcomes = {}, []

    def token_for(master):
        with lock:
            if master not in clients:
                client = client_factory()
                client._apply_credentials(master)
                clients[master] = client
            token = clients[master].get_bearer_token()
            if not token:
                raise ValueError('authentication_unavailable')
            return token

    def fetch(key, masters):
        prior = prior_accounts.get(key)
        base = {field: '' for field in COLUMNS}
        base.update(schema_version='1', source=source, record_type='account', account_id=key,
                    account_title=titles[key], snapshot_at=stamp, last_attempt_at=stamp,
                    last_success_at=(prior or {}).get('last_success_at', ''),
                    account_metadata_json=(prior or {}).get('account_metadata_json', '{}'))
        attempts, tasks, success, error = [], [], False, ''
        master = ''
        try:
            if not re.fullmatch(r'[A-Z]+\d+', key):
                raise ValueError('invalid_account_number')
            masters = masters - {''}
            if len(masters) > 1:
                raise ValueError('conflicting_master_accounts')
            master = next(iter(masters), (prior or {}).get('master_account', ''))
            base['master_account'] = master
            if master not in IBKRWebAPI.MASTER_ACCOUNT_CREDENTIALS:
                raise ValueError('unsupported_master_account' if master else 'missing_master_account')
            contexts = [master] + [m for m in IBKRWebAPI.MASTER_ACCOUNT_CREDENTIALS if m != master][:1]
            response = None
            for context in contexts:
                attempt = {'credential_master': context}
                attempts.append(attempt)
                token = token_for(context)
                pacer.wait()
                response = get(f'https://api.ibkr.com/gw/api/v1/accounts/{key}/tasks',
                               params={'type': source}, headers={'Authorization': f'Bearer {token}'}, timeout=(10, 45))
                attempt['http_status'] = response.status_code
                base.update(credential_master=context, http_status=str(response.status_code))
                if response.status_code != 403:
                    break
            if response.status_code != 200:
                raise ValueError(f'http_{response.status_code}')
            payload = response.json()
            if not isinstance(payload, dict) or _text(payload.get('accountId')).upper() != key:
                raise ValueError('tasks_account_mismatch')
            if any(payload.get(field) for field in ('error', 'errors', 'errorCode')):
                raise ValueError('tasks_provider_error')
            inventory = payload.get(SOURCES[source])
            present = payload.get(f'{source}TaskPresent')
            if inventory is None and present is False:
                inventory = []
            if not isinstance(inventory, list) or any(not isinstance(task, dict) for task in inventory):
                raise ValueError('invalid_task_inventory')
            if present is False and inventory:
                raise ValueError('contradictory_task_inventory')
            if payload.get('masterAccountId') and _text(payload['masterAccountId']).upper() != master:
                raise ValueError('tasks_master_mismatch')
            tasks = [_safe(task) for task in inventory]
            _json(tasks)
            metadata = _safe({field: value for field, value in payload.items() if field != SOURCES[source]})
            base.update(account_metadata_json=_json(metadata), last_success_at=datetime.now(timezone.utc).isoformat())
            success = True
        except Exception as exc:
            # Provider text/headers/URLs and credentials never enter the report.
            error = str(exc) if isinstance(exc, ValueError) and re.fullmatch(r'[a-z_0-9]+', str(exc)) else type(exc).__name__
            if attempts and 'http_status' not in attempts[-1]:
                attempts[-1]['error'] = error
        base.update(error=error, attempts_json=_json(attempts),
                    refresh_status='fresh' if success else 'stale' if base['last_success_at'] else 'unavailable')
        if success:
            base['task_count'] = str(len(tasks))
        else:
            base['task_count'] = (prior or {}).get('task_count', '')
        rows = [base]
        if success:
            for index, task in enumerate(tasks):
                row = dict(base, record_type='task', task_index=str(index), task_json=_json(task))
                fields = {'task_number': 'taskNumber', 'form_number': 'formNumber', 'form_name': 'formName',
                          'entity_id': 'entityId', 'external_id': 'externalId', 'state': 'state',
                          'action': 'action', 'date_completed': 'dateCompleted', 'online_task': 'onlineTask'}
                row.update({name: _text(task.get(field)) for name, field in fields.items()})
                row['is_completed'] = str(task.get('isCompleted') is True or task.get('isComplete') is True
                                          or _text(task.get('state')).lower() in ('complete', 'completed')).lower()
                row['required_for_approval'] = _text(task.get('isRequiredForApproval', task.get('requiredForApproval')))
                row['required_for_trading'] = _text(task.get('isRequiredForTrading', task.get('requiredForTrading')))
                rows.append(row)
        else:
            for previous_row in prior_tasks.get(key, []):
                rows.append({**previous_row, **{field: base[field] for field in ACCOUNT_FIELDS}})
        return rows, {'account_id': key, 'status': 'success' if success else 'failed',
                      'error': error, 'retained_previous': bool(prior), 'attempts': attempts}

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(fetch, key, masters) for key, masters in sorted(candidates.items())]
        for future in as_completed(futures):
            rows, outcome = future.result()
            output[outcome['account_id']] = rows
            outcomes.append(outcome)
            if progress:
                progress(outcome)
    rows = [row for key in sorted(output) for row in output[key]]
    rows = decode_snapshot(encode_snapshot(rows), source)
    counts = Counter(row['refresh_status'] for row in rows if row['record_type'] == 'account')
    report = {'status': 'success' if counts['fresh'] == len(candidates) else 'partial',
              'source': source, 'started_at': stamp, 'seconds': round(time.monotonic() - started, 3),
              'summary': {'total': len(candidates), 'successful': counts['fresh'],
                          'failed': counts['stale'] + counts['unavailable'], 'retained_stale': counts['stale'],
                          'unavailable': counts['unavailable'], 'task_rows': sum(row['record_type'] == 'task' for row in rows)},
              'outcomes': sorted(outcomes, key=lambda item: item['account_id'])}
    return rows, report


def snapshot_view(rows, source, account_id=None, *, status='all', offset=0, limit=500):
    if source not in SOURCES or status not in ('all', 'pending', 'completed'):
        raise ValueError('Invalid task report filter')
    if account_id is not None and not re.fullmatch(r'[A-Z]+\d+', account_id):
        raise ValueError('Invalid task report account')
    if offset < 0 or not 1 <= limit <= 2000:
        raise ValueError('Invalid task report pagination')
    selected = [row for row in rows if account_id is None or row['account_id'] == account_id]
    coverage = [{**{field: row[field] for field in COLUMNS[:15]},
                 'task_count': int(row['task_count']) if row['task_count'] else None,
                 'attempts': json.loads(row['attempts_json']),
                 'account_metadata': json.loads(row['account_metadata_json'])}
                for row in selected if row['record_type'] == 'account']
    tasks = [row for row in selected if row['record_type'] == 'task'
             and (status == 'all' or (row['is_completed'] == 'true') == (status == 'completed'))]
    for account in coverage:
        succeeded = account['last_success_at']
        age = (datetime.now(timezone.utc) - datetime.fromisoformat(succeeded)).total_seconds() if succeeded else None
        account['stale'] = account['refresh_status'] != 'fresh' or age is None or age < 0 or age > 86400
    return {'source': source, 'account_id': account_id, 'status': status,
            'coverage': coverage, 'summary': {'accounts': len(coverage), 'fresh': sum(not row['stale'] for row in coverage),
                                            'stale_or_unavailable': sum(row['stale'] for row in coverage), 'task_rows': len(tasks)},
            'offset': offset, 'limit': limit, 'total': len(tasks),
            'rows': [{**{field: row[field] for field in COLUMNS
                         if field not in ('task_json', 'account_metadata_json', 'attempts_json')},
                      'task': json.loads(row['task_json'])} for row in tasks[offset:offset + limit]]}
