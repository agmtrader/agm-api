#!/usr/bin/env python3
"""Read-only IBKR discovery/details benchmark. Writes only new local JSON files.

Default: 20 open accounts, sampled round-robin across configured masters.
--limit 0 selects all discovered accounts; --statuses ANOCPRQE includes all states.
No retries, database writes, Drive uploads, or production ETL invocation.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
import threading
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def account_rows(payload):
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ('accounts', 'data', 'value', 'result'):
            if key in payload:
                return account_rows(payload[key])
    raise ValueError('Unrecognized account-status response shape')


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


def write_json(path, value):
    # Never overwrite a previous snapshot; keep client data private locally.
    with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'w') as f:
        json.dump(value, f, indent=2, default=str)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--limit', type=int, default=20)
    parser.add_argument('--workers', type=int, default=4)
    parser.add_argument('--rate', type=float, default=5)
    parser.add_argument('--statuses', default='O')
    parser.add_argument('--start-date', default='1970-01-01', help='Account creation window start')
    parser.add_argument('--end-date', default=datetime.now(timezone.utc).date().isoformat())
    args = parser.parse_args()
    if args.limit < 0 or not 1 <= args.workers <= 10 or not 0 < args.rate <= 8:
        parser.error('limit >= 0, workers 1..10, rate > 0 and <= 8 required')
    if not args.statuses or any(s not in 'ANOCPRQE' for s in args.statuses):
        parser.error('statuses must contain only ANOCPRQE')
    from dotenv import load_dotenv
    load_dotenv(ROOT / '.env')
    import requests
    from src.utils.connectors.ibkr_web_api import IBKRWebAPI

    started = time.monotonic()
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    out = ROOT / 'dev' / 'ibkr_details_runs'
    out.mkdir(exist_ok=True, mode=0o700)
    details_path = out / f'account_details_{stamp}.json'
    report_path = out / f'account_details_{stamp}.report.json'
    clients = {}
    groups = {}
    discovery = []
    pacer = Pacer(args.rate)
    for master in sorted(IBKRWebAPI.MASTER_ACCOUNT_CREDENTIALS):
        client = IBKRWebAPI()
        # Only this client's main thread changes credentials/tokens. Workers
        # use immutable bearer strings, never the shared mutable connector.
        client._apply_credentials(master)
        clients[master] = client
        rows_by_id = {}
        for status in dict.fromkeys(args.statuses):
            offset = 0
            while True:
                token = client.get_bearer_token()
                if not token:
                    raise RuntimeError(f'Token unavailable for {master}')
                pacer.wait()
                response = requests.get(f'{client.BASE_URL}/gw/api/v1/accounts/status',
                    params={'status': status, 'limit': 1000, 'offset': offset,
                            'startDate': args.start_date, 'endDate': args.end_date},
                    headers={'Authorization': f'Bearer {token}'}, timeout=(10, 45))
                if response.status_code != 200:
                    raise RuntimeError(f'Discovery {master}/{status}: HTTP {response.status_code}')
                rows = account_rows(response.json())
                previous = len(rows_by_id)
                for row in rows:
                    account_id = str(row.get('accountId') or '').strip().upper()
                    if not account_id:
                        raise ValueError('Discovery row missing accountId')
                    returned_master = str(row.get('masterAccountId') or master).strip().upper()
                    if returned_master != master:
                        raise ValueError(f'Discovery master mismatch for {account_id}')
                    rows_by_id[account_id] = row
                discovery.append({'master': master, 'status': status, 'offset': offset, 'rows': len(rows)})
                if len(rows) < 1000:
                    break
                if len(rows_by_id) == previous:
                    raise ValueError('Discovery pagination made no progress')
                offset += len(rows)
        groups[master] = sorted(rows_by_id)
    selected = []
    while any(groups.values()) and (args.limit == 0 or len(selected) < args.limit):
        for master in groups:
            if groups[master] and (args.limit == 0 or len(selected) < args.limit):
                selected.append((master, groups[master].pop(0)))
    discovery_seconds = time.monotonic() - started
    print(f'Discovered {sum(x["rows"] for x in discovery)} rows; selected {len(selected)} accounts', flush=True)
    details, outcomes = [], []
    fetch_start = time.monotonic()

    def fetch(master, account_id, token):
        pacer.wait()
        tick = time.monotonic()
        result = {'account_id': account_id, 'master': master}
        try:
            response = requests.get(f'https://api.ibkr.com/gw/api/v1/accounts/{account_id}/details',
                headers={'Authorization': f'Bearer {token}'}, timeout=(10, 45))
            result['http_status'] = response.status_code
            if response.status_code != 200:
                raise ValueError(f'HTTP {response.status_code}')
            detail = response.json()
            if not isinstance(detail, dict) or detail.get('account', {}).get('accountId') != account_id:
                raise ValueError('Details account identity mismatch')
            if detail['account'].get('masterAccountId') != master:
                raise ValueError('Details master identity mismatch')
            result['status'] = 'success'
        except Exception as exc:
            detail = None
            result.update(status='failed', error=str(exc) if isinstance(exc, ValueError) else type(exc).__name__)
        result['seconds'] = round(time.monotonic() - tick, 3)
        return detail, result

    # Small waves keep short-lived bearer tokens fresh without sharing token
    # mutation with workers. Every failure is recorded, never retried.
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        for offset in range(0, len(selected), args.workers):
            wave = selected[offset:offset + args.workers]
            tokens = {m: clients[m].get_bearer_token() for m in dict.fromkeys(m for m, _ in wave)}
            if not all(tokens.values()):
                raise RuntimeError('Token unavailable before fetch wave')
            futures = [pool.submit(fetch, m, a, tokens[m]) for m, a in wave]
            for future in as_completed(futures):
                detail, result = future.result()
                outcomes.append(result)
                if detail is not None:
                    details.append(detail)
                print(f'{len(outcomes)}/{len(selected)} {result["account_id"]} {result["status"]} {result["seconds"]}s', flush=True)
            details.sort(key=lambda d: d['account']['accountId'])
            # A new checkpoint name for each completed wave.
            write_json(out / f'checkpoint_{stamp}_{offset}.json', details)
    report = {'started_utc': stamp, 'statuses': args.statuses, 'workers': args.workers,
        'requests_per_second': args.rate, 'creation_window': [args.start_date, args.end_date], 'discovery': discovery,
        'selected': len(selected), 'successful': len(details), 'failed': len(outcomes)-len(details),
        'discovery_seconds': round(discovery_seconds, 3),
        'fetch_seconds': round(time.monotonic()-fetch_start, 3),
        'total_seconds': round(time.monotonic()-started, 3), 'outcomes': outcomes,
        'coverage': 'Configured credential masters only; does not prove coverage of other masters.'}
    write_json(details_path, details)
    write_json(report_path, report)
    print(json.dumps({k:v for k,v in report.items() if k not in ('outcomes','discovery')}, indent=2))
    print(f'DETAILS_FILE={details_path}\nREPORT_FILE={report_path}')
    return 1 if report['failed'] or not selected else 0


if __name__ == '__main__':
    raise SystemExit(main())
