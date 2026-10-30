"""Read-only projections of the three saved task sources; never call IBKR."""
from collections import Counter
from datetime import datetime, timezone
import json
import re

SOURCES = ('compliance', 'pending', 'registration')


def freshness(status, succeeded):
    try:
        age = (datetime.now(timezone.utc) - datetime.fromisoformat(succeeded.replace('Z', '+00:00'))).total_seconds()
        return 'fresh' if status == 'fresh' and 0 <= age <= 86400 else 'stale'
    except (TypeError, ValueError, AttributeError):
        return 'unavailable' if status == 'unavailable' else 'stale'


def state_group(value):
    value = str(value or '').strip().lower()
    if value in ('complete', 'completed', 'done'):
        return 'completed'
    if re.search(r'received.*process|processing|under review', value):
        return 'processing'
    if re.search(r'not.*received|to be|to complete|pending|incomplete', value):
        return 'pending'
    return value


def build_review(feeds, health, detail=None):
    """Keep source observations separate; cross-source instance links are evidence only."""
    rows, accounts, raw = [], {}, {}

    def account(key, title='', master=''):
        value = accounts.setdefault(key, {'account_id': key, 'account_title': title, 'master_account': master,
                                         'coverage': {}, 'restriction': 'unknown', 'pending_count': 0, 'completed_count': 0})
        if title and not value['account_title']:
            value['account_title'] = title
        if master and not value['master_account']:
            value['master_account'] = master
        return value

    for source in ('pending', 'registration'):
        for row in feeds.get(source, []):
            a = account(row['account_id'], row['account_title'], row['master_account'])
            if row['record_type'] == 'account':
                a['coverage'][source] = {'status': freshness(row['refresh_status'], row['last_success_at']),
                    'refresh_status': row['refresh_status'], 'error': row['error'],
                    'last_success_at': row['last_success_at'], 'last_attempt_at': row['last_attempt_at'],
                    'task_count': int(row['task_count']) if row['task_count'] else None}
                continue
            key = f"{source}:{row['account_id']}:{row['task_index']}"
            completed = row['is_completed'] == 'true'
            state = row['state'] or 'Unknown'
            normalized = {'id': key, 'source': source, 'account_id': row['account_id'],
                'account_title': row['account_title'], 'master_account': row['master_account'],
                'title': row['form_name'] or 'Unnamed task', 'task_number': row['task_number'],
                'form_number': row['form_number'], 'person': row['entity_id'] or row['external_id'],
                'state': state, 'action': row['action'], 'completed': completed,
                'freshness': freshness(row['refresh_status'], row['last_success_at']),
                'last_success_at': row['last_success_at'], 'restriction': 'unknown',
                'date': row['date_completed'], 'warning': '', 'related_ids': []}
            rows.append(normalized)
            if key == detail:
                raw[key] = {'record': json.loads(row['task_json']), 'coverage': a['coverage'].get(source),
                            'credential_master': row['credential_master'], 'attempts': json.loads(row['attempts_json'])}
    compliance = feeds.get('compliance')
    for index, row in enumerate((compliance or {}).get('rows', [])):
        a = account(row['account_number'], row['account_title'])
        restriction = 'restricted' if row['account_restricted_status'] == 'RESTRICTED' else 'unrestricted'
        a['restriction'] = restriction
        a['coverage']['compliance'] = {'status': 'stale' if compliance['freshness']['stale'] else 'fresh',
            'last_success_at': compliance['source']['uploaded_at'], 'task_count': 0,
            'error': '', 'refresh_status': 'manual'}
        key = f"compliance:{row['account_number']}:{row['task_number']}"
        rows.append({'id': key, 'source': 'compliance', 'account_id': row['account_number'],
            'account_title': row['account_title'], 'master_account': a['master_account'],
            'title': row['form_name'], 'task_number': row['task_number'], 'form_number': str(row['form_number']),
            'person': '', 'state': row['task_status'], 'action': 'IBKR Portal',
            'completed': state_group(row['task_status']) == 'completed',
            'freshness': 'stale' if compliance['freshness']['stale'] else 'fresh',
            'last_success_at': compliance['source']['uploaded_at'], 'restriction': restriction,
            'date': row['date_assigned'], 'warning': '', 'related_ids': []})
        if key == detail:
            raw[key] = {'record': row, 'source': compliance['source'], 'freshness': compliance['freshness']}
    identities = {}
    for row in rows:
        if row['source'] in ('pending', 'compliance') and row['task_number']:
            identities.setdefault((row['account_id'], row['task_number']), {}).setdefault(row['source'], []).append(row)
    for variants in identities.values():
        pending, manual = variants.get('pending', []), variants.get('compliance', [])
        if pending and manual:
            exact = len(pending) == len(manual) == 1 and pending[0]['form_number'] == manual[0]['form_number']
            for row in pending + manual:
                row['warning'] = ('Source states differ; review both records.' if exact and state_group(pending[0]['state']) != state_group(manual[0]['state'])
                                  else 'Same task instance appears in both sources; observations kept separately.' if exact
                                  else 'Ambiguous task identity; source records kept separately.')
                if exact:
                    row['related_ids'] = [other['id'] for other in pending + manual if other is not row]
    for row in rows:
        a = accounts[row['account_id']]
        row['master_account'] = row['master_account'] or a['master_account']
        row['restriction'] = a['restriction']
        a['completed_count' if row['completed'] else 'pending_count'] += 1
        if row['source'] == 'compliance':
            a['coverage']['compliance']['task_count'] += 1
    for a in accounts.values():
        for source in SOURCES:
            if source not in a['coverage']:
                # Portal inventory absence cannot establish account-wide completeness.
                a['coverage'][source] = {'status': 'unavailable' if health[source]['status'] == 'unavailable' else 'not_listed',
                    'task_count': None, 'error': 'source_unavailable' if health[source]['status'] == 'unavailable' else '',
                    'last_success_at': None}
        api_status = [a['coverage'][source]['status'] for source in ('pending', 'registration')]
        a['freshness'] = 'unavailable' if 'unavailable' in api_status or 'not_listed' in api_status else 'stale' if 'stale' in api_status else 'fresh'
    return rows, accounts, raw


def review_view(feeds, health, args):
    view, status, source = args.get('view', 'tasks'), args.get('status', 'pending'), args.get('source', 'all')
    fresh, restriction = args.get('freshness', 'all'), args.get('restriction', 'all')
    offset, limit = int(args.get('offset', '0')), int(args.get('limit', '50'))
    query, master = args.get('search', '').strip().lower(), args.get('master_account', '')
    if (view not in ('tasks', 'accounts') or status not in ('pending', 'completed', 'all') or source not in ('all', *SOURCES)
            or fresh not in ('all', 'fresh', 'stale', 'unavailable') or restriction not in ('all', 'restricted', 'unrestricted', 'unknown')
            or offset < 0 or not 1 <= limit <= 200 or len(query) > 200 or (master and not re.fullmatch(r'[A-Z]+\d+|unknown', master))):
        raise ValueError('Invalid task review filters')
    detail = args.get('detail')
    rows, accounts, raw = build_review(feeds, health, detail)
    if detail is not None:
        if detail not in raw:
            return {'schema_version': 1, 'detail': None, 'sources': health}
        return {'schema_version': 1, 'detail': raw[detail], 'sources': health}

    def common(row, account_view=False):
        coverage = row.get('coverage', {})
        row_fresh = coverage.get(source, {}).get('status', 'unavailable') if account_view and source != 'all' else row['freshness']
        return ((source == 'all' or account_view or row['source'] == source)
            and (not master or (row['master_account'] or 'unknown') == master)
            and (fresh == 'all' or row_fresh == fresh)
            and (restriction == 'all' or row['restriction'] == restriction)
            and (not query or query in ' '.join(str(row.get(k) or '') for k in ('account_id', 'account_title', 'title', 'task_number', 'form_number', 'person', 'state', 'action')).lower()))
    filtered = [row for row in rows if common(row)]
    counts = Counter('completed' if row['completed'] else 'pending' for row in filtered)
    selected = [row for row in filtered if status == 'all' or row['completed'] == (status == 'completed')]
    selected.sort(key=lambda row: (row['account_id'], row['title'].lower(), row['source'], row['id']))
    account_rows = sorted((a for a in accounts.values() if common(a, True)), key=lambda row: row['account_id'])
    result_rows = selected if view == 'tasks' else account_rows
    return {'schema_version': 1, 'view': view, 'status': status, 'sources': health,
        'masters': sorted({a['master_account'] for a in accounts.values() if a['master_account']}),
        'total': len(result_rows), 'offset': offset, 'limit': limit,
        'summary': {'pending': counts['pending'], 'completed': counts['completed'],
                    'accounts': len({row['account_id'] for row in selected}) if view == 'tasks' else len(account_rows),
                    'coverage_gaps': sum(a['freshness'] != 'fresh' for a in account_rows),
                    'inventory_accounts': len(accounts)},
        'rows': result_rows[offset:offset + limit]}
