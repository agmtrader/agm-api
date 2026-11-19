#!/usr/bin/env python3
"""Time IBKR snapshots for the published bond-report universe.

From agm-api:
    PYTHONDONTWRITEBYTECODE=1 venv/bin/python -B operations/benchmark_bond_market_data.py

Uses the same Drive reports, ADC/Secret Manager credentials and snapshot fields
as the application, without starting Flask or publishing ETL resources. One
brokerage session serves the whole run. Each batch polls until quote coverage
stabilizes, then confirms subscription cleanup before requesting the next batch.
Polling deadlines and unavailable fields are explicit; no fixed warmup sleep.
IBKR permits at most 100 conids per snapshot request:
https://www.interactivebrokers.com/campus/ibkr-api-page/web-api-changelog/

JSON goes to stdout; progress and connector logs go to stderr. --details includes
every contract's status and returned snapshot. Missing fields are reported, never filled
from the stored report. Exit 0 means all selected contracts returned nonempty
snapshots; it does not imply every requested field or executable quote exists.
Exit 2 means invalid input, failed/incomplete retrieval, or interruption.
"""
from __future__ import annotations

import argparse
from collections import Counter
from contextlib import redirect_stdout
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import re
import sys
from time import perf_counter

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

QUOTE_FIELDS = (
    'BID_PRICE', 'ASK_PRICE', 'LAST_PRICE', 'BID_SIZE', 'ASK_SIZE',
    'BID_YIELD', 'ASK_YIELD', 'LAST_YIELD',
)


def present(value):
    return value is not None and str(value).strip().lower() not in (
        '', 'none', 'nan', 'n/a', 'null', '-', '--',
    )


def contract_id(value):
    match = re.fullmatch(r'(?:IBCID)?([1-9][0-9]*)', str(value).strip())
    return match.group(1) if match else None


def collect_contracts(reports):
    """Reject unidentified/conflicting rows, and deduplicate in report order."""
    contracts, sources, invalid = {}, {}, []
    for name, rows in reports.items():
        if not isinstance(rows, list) or not rows:
            raise ValueError(f'{name}: expected a nonempty list of reporting rows')
        sources[name] = {'rows': len(rows), 'unique_contracts': 0}
        source_ids = set()
        for index, row in enumerate(rows):
            if not isinstance(row, dict):
                invalid.append({'source': name, 'row': index + 1, 'reason': 'not an object'})
                continue
            candidates = [row[key] for key in ('conid', 'Conid', 'ConID', 'con_id', 'Contract ID')
                          if present(row.get(key))]
            if str(row.get('Symbol', '')).strip().startswith('IBCID'):
                candidates.append(row['Symbol'])
            ids = {contract_id(value) for value in candidates}
            if len(ids) != 1 or None in ids:
                invalid.append({'source': name, 'row': index + 1,
                                'reason': 'missing, invalid or conflicting contract ID'})
                continue
            conid = ids.pop()
            source_ids.add(conid)
            contracts.setdefault(conid, {'conid': conid, 'sources': [], 'status': 'not_requested'})
            if name not in contracts[conid]['sources']:
                contracts[conid]['sources'].append(name)
        sources[name]['unique_contracts'] = len(source_ids)
    return contracts, sources, invalid


def reconcile(batch, payload):
    if not isinstance(payload, list):
        raise ValueError('Snapshot response is not a list')
    snapshots = {}
    for row in payload:
        if not isinstance(row, dict):
            raise ValueError('Snapshot response contains a non-object row')
        conid = contract_id(row.get('conid'))
        if conid not in batch or conid in snapshots:
            raise ValueError('Snapshot response has an unidentified, unexpected or duplicate contract')
        snapshots[conid] = row
    results = {}
    for conid in batch:
        row = snapshots.get(conid)
        fields = [key for key in QUOTE_FIELDS if row and present(row.get(key))]
        if row is None:
            status = 'missing'
        elif row.get('error'):
            status = 'provider_error'
        elif not any(present(row.get(key)) for key in ('SYMBOL', 'COMPANY_NAME', *QUOTE_FIELDS)):
            status = 'empty'
        else:
            status = 'returned'
        results[conid] = {'status': status, 'quote_fields_present': fields, 'snapshot': row}
    return results


def benchmark(args):
    started = perf_counter()
    result = {'started_at_utc': datetime.now(timezone.utc).isoformat(),
              'batch_size': args.batch_size, 'session_mode': 'single',
              'poll_interval_seconds': args.poll_interval, 'poll_timeout_seconds': args.poll_timeout,
              'settle_seconds': args.settle_seconds,
              'limited_run': args.limit is not None, 'batches': [], 'timings_seconds': {}}
    contracts = {}
    stage = 'imports'
    try:
        os.environ.setdefault('DEV_MODE', 'false')
        # Keep JSON output usable even if local DEV_MODE enables Rich logging.
        with redirect_stdout(sys.stderr):
            from src.components.tools.public.reporting import get_bond_report, get_ust_bond_report
            from src.utils.connectors.ibkr_trading_api import IBKRTradingAPI
        stage = 'report_loading'
        source_started = perf_counter()
        reports = {'corporate_bonds': get_bond_report()}
        if args.include_treasuries:
            reports['treasuries'] = get_ust_bond_report()
        contracts, sources, invalid = collect_contracts(reports)
        result['timings_seconds']['report_loading'] = perf_counter() - source_started
        result.update(sources=sources, invalid_rows=invalid, universe_contracts=len(contracts))
        if invalid:
            raise ValueError(f'{len(invalid)} reporting rows have invalid contract IDs; no IBKR requests made')
        if args.limit:
            contracts = dict(list(contracts.items())[:args.limit])
        if not contracts:
            raise ValueError('Reporting universe has no valid contract IDs')
        print(f"Loaded {len(contracts)} unique contracts; batch size {args.batch_size}", file=sys.stderr, flush=True)
        if not args.dry_run:
            stage = 'connector_setup'
            setup_started = perf_counter()
            api = IBKRTradingAPI()
            result['timings_seconds']['connector_setup'] = perf_counter() - setup_started
            ids = list(contracts)
            stage = 'session_setup'
            session_started = perf_counter()
            with api.market_data_session():
                result['timings_seconds']['session_setup_total'] = perf_counter() - session_started
                context = api._market_data_context
                result['session_readiness'] = dict(context.get('readiness', {}))
                try:
                    for offset in range(0, len(ids), args.batch_size):
                        batch = ids[offset:offset + args.batch_size]
                        receipt = {'batch': len(result['batches']) + 1, 'requested': len(batch),
                                   'status': 'started'}
                        result['batches'].append(receipt)
                        batch_started = perf_counter()
                        recovery_start, failure_start = len(context['recoveries']), len(context['failures'])
                        stage = 'snapshot'
                        snapshot_started = perf_counter()
                        failed = False
                        try:
                            payload = api.get_market_data_snapshot(','.join(batch),
                                poll_interval=args.poll_interval, timeout=args.poll_timeout,
                                settle_seconds=args.settle_seconds)
                            updates = reconcile(batch, payload)
                            for conid, update in updates.items():
                                contracts[conid].update(update)
                            receipt['status'] = 'processed'
                        except (Exception, KeyboardInterrupt):
                            failed = True
                            receipt['status'] = 'failed'
                            for conid in batch:
                                if contracts[conid]['status'] == 'not_requested':
                                    contracts[conid]['status'] = 'failed'
                            raise
                        finally:
                            receipt['snapshot_seconds'] = perf_counter() - snapshot_started
                            receipt['polling'] = dict(context.get('snapshot_polling', {}))
                            receipt['session_recoveries'] = list(context['recoveries'][recovery_start:])
                            receipt['session_failures'] = list(context['failures'][failure_start:])
                            cleanup_started = perf_counter()
                            if not failed:
                                stage = 'subscription_cleanup'
                            try:
                                receipt['subscription_cleanup'] = api.unsubscribe_all_market_data()
                            except Exception as cleanup_error:
                                receipt['cleanup_error'] = {'type': type(cleanup_error).__name__,
                                                           'code': getattr(cleanup_error, 'code', None)}
                                if not failed:
                                    receipt['status'] = 'cleanup_failed'
                                    raise
                            finally:
                                receipt['cleanup_seconds'] = perf_counter() - cleanup_started
                                receipt['total_seconds'] = perf_counter() - batch_started
                        print(f"Batch {receipt['batch']}: {len(batch)} contracts in {receipt['total_seconds']:.2f}s "
                              f"({receipt['polling'].get('requests', 0)} polls, "
                              f"deadline={receipt['polling'].get('timed_out', False)})", file=sys.stderr, flush=True)
                finally:
                    result['session_recoveries'] = list(context['recoveries'])
                    result['session_failures'] = list(context['failures'])
    except (Exception, KeyboardInterrupt) as error:
        diagnostics = {}
        if stage != 'imports':
            from src.utils.error_diagnostics import safe_upstream_details
            diagnostics = safe_upstream_details(getattr(error, 'details', {}))
        result['error'] = {'stage': stage, 'type': type(error).__name__,
                           'message': str(error), 'code': getattr(error, 'code', None),
                           'provider_diagnostics': diagnostics}
    result['timings_seconds']['total'] = perf_counter() - started
    result['timings_seconds']['snapshot_total'] = sum(batch.get('snapshot_seconds', 0) for batch in result['batches'])
    result['timings_seconds']['subscription_cleanup_total'] = sum(batch.get('cleanup_seconds', 0) for batch in result['batches'])
    result['polling_deadline_batches'] = sum(batch.get('polling', {}).get('timed_out', False) for batch in result['batches'])
    counts = Counter(row['status'] for row in contracts.values())
    result.update(selected_contracts=len(contracts), statuses=dict(counts),
                  quote_field_coverage={field: sum(row['status'] == 'returned' and field in row.get('quote_fields_present', [])
                                                   for row in contracts.values()) for field in QUOTE_FIELDS},
                  both_bid_and_ask=sum(row['status'] == 'returned' and all(field in row.get('quote_fields_present', [])
                                       for field in ('BID_PRICE', 'ASK_PRICE')) for row in contracts.values()),
                  incomplete_contracts=sum(row['status'] != 'returned' for row in contracts.values()))
    result['complete'] = bool(contracts) and not result.get('error') and counts['returned'] == len(contracts)
    result['dry_run'] = args.dry_run
    if args.details:
        result['contracts'] = list(contracts.values())
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--batch-size', type=int, default=100, help='Contracts per snapshot batch (1–100; default 100)')
    parser.add_argument('--poll-interval', type=float, default=0.25, help='Seconds between snapshot polls (default 0.25; minimum 0.1)')
    parser.add_argument('--poll-timeout', type=float, default=10, help='Maximum polling window per batch (default 10 seconds)')
    parser.add_argument('--settle-seconds', type=float, default=0.75, help='How long field coverage must stabilize (default 0.75 seconds)')
    parser.add_argument('--include-treasuries', action='store_true', help='Also read the Treasury reporting universe')
    parser.add_argument('--limit', type=int, help='Benchmark only the first N deduplicated contracts')
    parser.add_argument('--dry-run', action='store_true', help='Load/validate reports without opening an IBKR session')
    parser.add_argument('--details', action='store_true', help='Include per-contract snapshots in JSON output')
    args = parser.parse_args()
    if not 1 <= args.batch_size <= 100:
        parser.error('--batch-size must be between 1 and 100')
    if args.limit is not None and args.limit <= 0:
        parser.error('--limit must be positive')
    if (not all(math.isfinite(value) for value in (args.poll_interval, args.poll_timeout, args.settle_seconds))
            or args.poll_interval < 0.1 or args.poll_timeout <= 0 or args.settle_seconds < 0):
        parser.error('Polling requires interval >= 0.1, timeout > 0 and settle-seconds >= 0')
    result = benchmark(args)
    print(json.dumps(result, indent=2, allow_nan=False))
    return 0 if result['complete'] or (args.dry_run and not result.get('error')) else 2


if __name__ == '__main__':
    sys.exit(main())
