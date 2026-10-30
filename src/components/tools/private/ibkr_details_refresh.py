"""Fresh IBKR reads with immutable credentials and one alternate read after 403."""
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import re
import threading
import time

import requests
from src.utils.connectors.ibkr_web_api import IBKRWebAPI


def _delinked_date(value):
    """Only a valid, nonfuture broker lifecycle date explains an absent master."""
    if not isinstance(value, str) or not re.fullmatch(r'\d{8}', value):
        return None
    try:
        day = datetime.strptime(value, '%Y%m%d').date()
    except ValueError:
        return None
    return value if day <= datetime.now(timezone.utc).date() else None


class Pacer:
    def __init__(self, rate):
        self.interval = 1 / rate
        self.next_start = 0
        self.lock = threading.Lock()

    def wait(self):
        with self.lock:
            now = time.monotonic()
            delay = max(0, self.next_start - now)
            self.next_start = max(now, self.next_start) + self.interval
        if delay:
            time.sleep(delay)


def refresh_details(accounts, previous, *, workers=4, rate=5, limit=0,
                    client_factory=IBKRWebAPI, get=requests.get, progress=None):
    if not 1 <= workers <= 10 or not 0 < rate <= 8 or limit < 0:
        raise ValueError('Invalid IBKR refresh workers/rate/limit')
    started = time.monotonic()
    stamp = datetime.now(timezone.utc).isoformat()
    by_id = {}
    for detail in previous:
        if not isinstance(detail, dict) or not isinstance(detail.get('account'), dict):
            raise ValueError('Malformed prior account details')
        key = str(detail['account'].get('accountId') or '').strip().upper()
        if not key or key in by_id:
            raise ValueError('Missing or duplicate prior account identity')
        by_id[key] = detail
    candidates = {}
    for row in accounts:
        key = str(row.get('ibkr_account_number') or '').strip().upper()
        if not key:
            continue
        candidates.setdefault(key, set()).add(str(row.get('master_account') or '').strip().upper())
    # Refresh prior-only accounts too, so old backup entries never imply freshness.
    for key, detail in by_id.items():
        if key not in candidates:
            candidates[key] = {str(detail['account'].get('masterAccountId') or '').strip().upper()}
    selected = sorted(candidates)
    if limit:
        selected = selected[:limit]
    report = {'started_at': stamp, 'population': len(candidates), 'selected': len(selected),
              'sampled': bool(limit), 'workers': workers, 'rate': rate, 'outcomes': []}
    eligible = []
    for key in selected:
        masters = candidates[key] - {''}
        reason = None
        if not re.fullmatch(r'[A-Z]+\d+', key):
            reason = 'invalid_account_number'
        elif len(masters) > 1:
            reason = 'conflicting_master_accounts'
        elif not masters:
            # Existing IBKR response is evidence of routing, not a guessed default.
            master = str(by_id.get(key, {}).get('account', {}).get('masterAccountId') or '').strip().upper()
            if master:
                masters = {master}
            else:
                reason = 'missing_master_account'
        if not reason and next(iter(masters)) not in IBKRWebAPI.MASTER_ACCOUNT_CREDENTIALS:
            reason = 'unsupported_master_account'
        if reason:
            report['outcomes'].append({'account_id': key, 'status': 'failed', 'error': reason,
                                       'retained_previous': key in by_id})
        else:
            eligible.append((key, next(iter(masters))))
    clients = {}
    credential_lock = threading.Lock()
    pacer = Pacer(rate)

    def token_for(credential_master):
        # A client stays bound to one credential set, including across workers.
        with credential_lock:
            if credential_master not in clients:
                client = client_factory()
                client._apply_credentials(credential_master)
                clients[credential_master] = client
            token = clients[credential_master].get_bearer_token()
            if not token:
                raise ValueError('token_unavailable')
            return token

    def fetch(key, master, token):
        tick = time.monotonic()
        outcome = {'account_id': key, 'master_account': master, 'status': 'failed',
                   'retained_previous': key in by_id, 'attempts': []}
        detail = None
        try:
            pacer.wait()
            response = get(f'https://api.ibkr.com/gw/api/v1/accounts/{key}/details',
                           headers={'Authorization': f'Bearer {token}'}, timeout=(10, 45))
            outcome['attempts'].append({'credential_master': master, 'http_status': response.status_code})
            outcome['credential_master'] = master
            if response.status_code == 403:
                alternate = next((m for m in IBKRWebAPI.MASTER_ACCOUNT_CREDENTIALS if m != master), None)
                if alternate:
                    outcome['http_status'] = 403
                    attempt = {'credential_master': alternate}
                    outcome['attempts'].append(attempt)
                    try:
                        alternate_token = token_for(alternate)
                    except Exception as exc:
                        attempt['error'] = f'authentication_{type(exc).__name__}'
                        raise ValueError(attempt['error']) from exc
                    pacer.wait()
                    outcome['credential_master'] = alternate
                    response = get(f'https://api.ibkr.com/gw/api/v1/accounts/{key}/details',
                                   headers={'Authorization': f'Bearer {alternate_token}'}, timeout=(10, 45))
                    attempt['http_status'] = response.status_code
                    outcome['credential_master'] = alternate
            outcome['http_status'] = response.status_code
            if response.status_code != 200:
                raise ValueError(f'HTTP {response.status_code}')
            candidate = response.json()
            if not isinstance(candidate, dict) or not isinstance(candidate.get('account'), dict):
                raise ValueError('invalid_details_shape')
            account = candidate['account']
            if str(account.get('accountId') or '').strip().upper() != key:
                raise ValueError('details_account_mismatch')
            returned_master = str(account.get('masterAccountId') or '').strip().upper()
            delinked = _delinked_date(account.get('dateDelinked'))
            if not returned_master and delinked:
                # IBKR returns a small tombstone after delinking. Keep prior
                # details as history, never replace them with a fresh tombstone.
                outcome.update(status='skipped', reason='account_delinked', date_delinked=delinked)
            elif returned_master != master:
                raise ValueError('details_master_mismatch')
            else:
                detail = dict(candidate)
                detail['_agm_refresh'] = {'status': 'fresh', 'last_success_at': datetime.now(timezone.utc).isoformat()}
                outcome.update(status='success', retained_previous=False)
        except Exception as exc:
            outcome['error'] = str(exc) if isinstance(exc, ValueError) else type(exc).__name__
            if outcome['attempts']:
                outcome['attempts'][-1]['error'] = outcome['error']
        outcome['seconds'] = round(time.monotonic() - tick, 3)
        return detail, outcome

    with ThreadPoolExecutor(max_workers=workers) as pool:
        for offset in range(0, len(eligible), workers):
            wave = eligible[offset:offset + workers]
            tokens = {}
            for master in dict.fromkeys(m for _, m in wave):
                try:
                    tokens[master] = token_for(master)
                except Exception as exc:
                    tokens[master] = None
                    for key, m in wave:
                        if m == master:
                            report['outcomes'].append({'account_id': key, 'master_account': master,
                                'status': 'failed', 'error': f'authentication_{type(exc).__name__}',
                                'retained_previous': key in by_id})
            futures = [pool.submit(fetch, key, master, tokens[master])
                       for key, master in wave if tokens[master]]
            for future in as_completed(futures):
                detail, outcome = future.result()
                report['outcomes'].append(outcome)
                if detail is not None:
                    by_id[outcome['account_id']] = detail
                if progress:
                    progress(outcome)
    outcomes = {o['account_id']: o for o in report['outcomes']}
    for key, detail in list(by_id.items()):
        if key in outcomes and outcomes[key]['status'] == 'success':
            continue
        metadata = detail.get('_agm_refresh') or {}
        outcome = outcomes.get(key, {})
        refresh = {
            'status': 'stale', 'last_success_at': metadata.get('last_success_at'),
            'last_attempt_at': stamp if key in outcomes else None,
            'error': outcome.get('error') or outcome.get('reason', 'not_selected_in_sample')}
        if outcome.get('reason') == 'account_delinked':
            refresh['date_delinked'] = outcome['date_delinked']
        by_id[key] = dict(detail, _agm_refresh=refresh)
    report['summary'] = {'total': len(selected),
        'successful': sum(o['status'] == 'success' for o in report['outcomes']),
        'failed': sum(o['status'] == 'failed' for o in report['outcomes']),
        'skipped': sum(o['status'] == 'skipped' for o in report['outcomes']),
        'delinked': sum(o.get('reason') == 'account_delinked' for o in report['outcomes']),
        'retained_stale': sum(d['_agm_refresh']['status'] == 'stale' for d in by_id.values())}
    report['status'] = 'success' if selected and not report['summary']['failed'] and not limit else 'partial'
    report['seconds'] = round(time.monotonic() - started, 3)
    report['outcomes'].sort(key=lambda o: o['account_id'])
    return [by_id[key] for key in sorted(by_id)], report
