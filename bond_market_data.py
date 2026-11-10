#!/usr/bin/env python3
"""Enrich scanner contracts using IBKR snapshots and full security definitions.

Writes resumable raw JSONL and an ETL-shaped CSV locally. Does not upload data.
Uses the same IBKR session credential as the scanner.
"""
import json
import math
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent
SCANNER = ROOT / 'bond_scanner_results.json'
RAW = ROOT / 'bond_market_data.jsonl'
CSV = ROOT / 'bond_market_data_etl_comparison.csv'
CHUNK_SIZE = 50


def load_records():
    records = {}
    if RAW.exists():
        with RAW.open(encoding='utf-8') as stream:
            for line in stream:
                if line.strip():
                    record = json.loads(line)
                    if 'run_started_utc' in record:
                        records = {}
                        continue
                    records[str(record['conid'])] = record
    return records


def write_comparison(records):
    import pandas as pd
    from src.components.tools.private.etl import process_bonds
    from src.components.tools.public.trade_tickets import extract_bond_details

    rows = []
    for record in records.values():
        snap = record.get('snapshot') or {}
        definition = record.get('definition') or {}
        if definition.get('currency') != 'USD':
            continue
        scanner = record.get('scanner') or {}
        symbol = snap.get('SYMBOL') or scanner.get('symbol') or ''
        description = snap.get('CONTRACT_DESCRIPTION_2') or scanner.get('contract_description_2') or ''
        instrument = (str(symbol) + ' ' + str(description)).strip()
        details = extract_bond_details(instrument) if instrument else {}
        last_match = re.search(r'[\d.]+', str(snap.get('LAST_PRICE') or ''))
        try:
            last = float(last_match.group()) if last_match else None
        except ValueError:
            last = None
        if last == 0 or (last is not None and not math.isfinite(last)):
            last = None
        coupon = details.get('coupon')
        row = {'Symbol': 'IBCID' + str(record['conid']), 'Financial Instrument': instrument,
               'Company Name': snap.get('COMPANY_NAME') or scanner.get('company_name'),
               'Bid Size': snap.get('BID_SIZE'), 'Bid': snap.get('BID_PRICE'),
               'Bid Yield': snap.get('BID_YIELD'), 'Ask Size': snap.get('ASK_SIZE'),
               'Ask': snap.get('ASK_PRICE'), 'Ask Yield': snap.get('ASK_YIELD'),
               'Industry': snap.get('INDUSTRY'), 'Sector': '',
               'Current Yield': (100 * coupon / last) if coupon is not None and last else None,
               'Maturity': details.get('maturity'), 'Next Option Date': '', 'Coupon': coupon,
               'Last': last, 'Ratings': snap.get('RATINGS'), 'Payment Frequency': '',
               'Trading Currency': 'USD', 'Issue Date': snap.get('ISSUE_DATE'),
               'Last Trading Date': snap.get('LAST_TRADING_DATE'),
               'Actual IBKR Currency': definition.get('currency'),
               'IBKR Bid Yield Raw': snap.get('BID_YIELD'),
               'IBKR Ask Yield Raw': snap.get('ASK_YIELD'),
               'IBKR Last Yield Raw': snap.get('LAST_YIELD'),
               'IBKR ISIN Parsed': details.get('isin'),
               'IBKR Coupon Type': (definition.get('bond') or {}).get('couponType'),
               'IBKR Definition Maturity': (definition.get('bond') or {}).get('maturityDate'),
               'Market Data Availability': snap.get('MARKET_DATA_AVAILABILITY'),
               'Snapshot Present': bool(snap), 'Definition Present': bool(definition),
               'ETL Would Exclude Missing Last': last is None}
        rows.append(row)
    if not rows:
        return
    source = pd.DataFrame(rows)
    result = process_bonds(source)
    for name in ('Actual IBKR Currency', 'IBKR Bid Yield Raw', 'IBKR Ask Yield Raw',
                 'IBKR Last Yield Raw', 'IBKR ISIN Parsed', 'IBKR Coupon Type',
                 'IBKR Definition Maturity', 'Market Data Availability', 'Snapshot Present',
                 'Definition Present', 'ETL Would Exclude Missing Last'):
        result[name] = source[name].values
    result.to_csv(CSV, index=False)


def enrich():
    from src.utils.connectors.ibkr_trading_api import IBKRTradingAPI
    data = json.loads(SCANNER.read_text())
    scanner = {str(row['con_id']): row for row in data['contracts'] if row.get('con_id') is not None}
    records = load_records()
    remaining = [key for key in scanner if key not in records]
    if not remaining and scanner:
        with RAW.open('a', encoding='utf-8') as stream:
            stream.write(json.dumps({'run_started_utc': datetime.now(timezone.utc).isoformat()}) + '\n')
        records = {}
        remaining = list(scanner)
    print(f'Enrichment: {len(records)} saved, {len(remaining)} remaining', flush=True)
    if not remaining:
        write_comparison(records)
        return 0
    try:
        with IBKRTradingAPI().market_data_session(credential='aguiagm2024') as api:
            for offset in range(0, len(remaining), CHUNK_SIZE):
                ids = remaining[offset:offset + CHUNK_SIZE]
                response = requests.get(api.BASE_URL + '/v1/api/trsrv/secdef',
                    headers=api._require_sso_headers(), params={'conids': ','.join(ids)}, timeout=(10, 90))
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict) or not isinstance(payload.get('secdef'), list):
                    raise ValueError('Unexpected security definition response')
                definitions = {str(item['conid']): item for item in payload['secdef']
                               if isinstance(item, dict) and item.get('conid') is not None}
                snapshots = api.get_market_data_snapshot(','.join(ids))
                if not isinstance(snapshots, list):
                    raise ValueError('Unexpected market snapshot response')
                snapshots = {str(item['conid']): item for item in snapshots
                             if isinstance(item, dict) and item.get('conid') is not None}
                timestamp = datetime.now(timezone.utc).isoformat()
                with RAW.open('a', encoding='utf-8') as stream:
                    for conid in ids:
                        record = {'conid': conid, 'retrieved_at_utc': timestamp, 'scanner': scanner[conid],
                                  'snapshot': snapshots.get(conid), 'definition': definitions.get(conid)}
                        stream.write(json.dumps(record, ensure_ascii=False, default=str) + '\n')
                        records[conid] = record
                print(f'Enriched {len(records)}/{len(scanner)}; batch definitions={len(definitions)}, snapshots={len(snapshots)}', flush=True)
    except Exception as error:
        print(f'Enrichment stopped: {error}', file=sys.stderr)
        write_comparison(records)
        return 1
    write_comparison(records)
    return 0


if __name__ == '__main__':
    sys.exit(enrich())
