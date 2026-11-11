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


def build_comparison(records):
    import pandas as pd
    from src.components.tools.private.etl import process_bonds, ratings
    from src.components.tools.public.trade_tickets import extract_bond_details

    rows = []
    for record in records.values():
        snap = record.get('snapshot') or {}
        definition = record.get('definition') or {}
        if definition.get('currency') != 'USD':
            continue
        if str((definition.get('bond') or {}).get('isDefaulted') or '').upper() in ('Y', 'T', 'TRUE'):
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
        bond = definition.get('bond') or {}
        maturity = bond.get('maturityDate')
        if maturity:
            try:
                maturity = datetime.strptime(str(maturity), '%Y%m%d').date().isoformat()
            except ValueError:
                maturity = details.get('maturity')
        else:
            maturity = details.get('maturity')
        asof = record['retrieved_at_utc'][:10].replace('-', '')
        active = [c for c in bond.get('coupon', []) if str(c.get('df') or '00000000') <= asof <= str(c.get('dt') or '99999999')]
        frequencies = {(c.get('dateCalcParams') or {}).get('frequency') for c in active}
        frequencies.discard(None)
        frequency = next(iter(frequencies)) if len(frequencies) == 1 else None
        frequency_text = {1: 'Annual', 2: 'Semi-Annual', 4: 'Quarterly', 12: 'Monthly', 0: 'Zero Coupon'}.get(frequency)
        option_dates = [str(option[key]) for option in bond.get('embeddedOpt', []) for key in ('ncdt', 'npdt') if option.get(key) and str(option[key]) >= asof and re.fullmatch(r'\d{8}', str(option[key]))]
        option_date = datetime.strptime(min(option_dates), '%Y%m%d').date().isoformat() if option_dates else None
        def yield_number(value):
            try:
                return float(str(value).strip().removesuffix('%')) if value not in (None, '') else None
            except ValueError:
                return None
        row = {'Symbol': 'IBCID' + str(record['conid']), 'Financial Instrument': instrument,
               'Company Name': snap.get('COMPANY_NAME') or scanner.get('company_name'),
               'Bid Size': snap.get('BID_SIZE'), 'Bid': snap.get('BID_PRICE'),
               'Bid Yield': yield_number(snap.get('BID_YIELD')), 'Ask Size': snap.get('ASK_SIZE'),
               'Ask': snap.get('ASK_PRICE'), 'Ask Yield': yield_number(snap.get('ASK_YIELD')),
               'Industry': snap.get('INDUSTRY'), 'Sector': definition.get('sector'),
               'Current Yield': (100 * coupon / last) if coupon is not None and last else None,
               'Maturity': maturity, 'Next Option Date': option_date, 'Coupon': coupon,
               'Last': last, 'Ratings': snap.get('RATINGS'), 'Payment Frequency': frequency_text,
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
        return pd.DataFrame()
    source = pd.DataFrame(rows)
    result = process_bonds(source)
    result['Bid Size'] = result['Bid Size'].astype(object)
    result['Ask Size'] = result['Ask Size'].astype(object)
    # Definitions contain agency-specific issue ratings; retain the source snapshot
    # text separately and use the ETL's conservative worst-recognized policy.
    sp_keys = {key.upper(): key for key, val in ratings.items() if val['Source'] == 'S&P'}
    moody_keys = {key.upper(): key for key, val in ratings.items() if val['Source'] == "Moody's"}
    # The shared dictionary's duplicated C/D keys resolve to Moody's; levels are identical.
    sp_keys.update({'C': 'C', 'D': 'D'})
    by_symbol = {'IBCID' + str(r['conid']): r for r in records.values()}
    for index, row in result.iterrows():
        record = by_symbol[row['Symbol']]
        snapshot = record.get('snapshot') or {}
        # Preserve source quote-size units; numeric coercion discards "$500K".
        result.at[index, 'Bid Size'] = snapshot.get('BID_SIZE')
        result.at[index, 'Ask Size'] = snapshot.get('ASK_SIZE')
        def usd_size(value):
            match = re.fullmatch(r'\$([\d,.]+)([KMB]?)', str(value or '').strip())
            if not match:
                return None
            return float(match.group(1).replace(',', '')) * {'': 1, 'K': 1000, 'M': 1000000, 'B': 1000000000}[match.group(2)]
        bid_size, ask_size = usd_size(snapshot.get('BID_SIZE')), usd_size(snapshot.get('ASK_SIZE'))
        result.at[index, 'Size Preasure'] = ask_size - bid_size if bid_size is not None and ask_size is not None else None
        issue_ratings = ((record.get('definition') or {}).get('bond') or {}).get('issueRating') or []
        resolved = {}
        for agency, mapping in (('MOODY', moody_keys), ('SP', sp_keys)):
            candidates = {mapping.get(str(item.get('ratingValue') or '').upper()) for item in issue_ratings if item.get('agency') == agency}
            candidates.discard(None)
            if len(candidates) == 1:
                resolved[agency] = candidates.pop()
        result.at[index, 'Moodys'] = resolved.get('MOODY')
        result.at[index, 'SP'] = resolved.get('SP')
        if resolved:
            result.at[index, 'Ratings'] = '/'.join(resolved[a] for a in ('MOODY', 'SP') if a in resolved) + ' (' + '/'.join(a for a in ('MOODY', 'SP') if a in resolved) + ')'
        levels = [ratings[value]['Level'] for value in resolved.values()]
        level = max(levels) if levels else None
        result.at[index, 'Rating Level'] = level
        result.at[index, 'S&P Equivalent'] = next((key for key, val in ratings.items() if val['Level'] == level and val['Source'] == 'S&P'), None) if level else None
        if level in (21, 22):
            result.at[index, 'S&P Equivalent'] = {21: 'C', 22: 'D'}[level]
    for name in ('Actual IBKR Currency', 'IBKR Bid Yield Raw', 'IBKR Ask Yield Raw',
                 'IBKR Last Yield Raw', 'IBKR ISIN Parsed', 'IBKR Coupon Type',
                 'IBKR Definition Maturity', 'Market Data Availability', 'Snapshot Present',
                 'Definition Present', 'ETL Would Exclude Missing Last'):
        result[name] = source[name].values
    return result


def write_comparison(records):
    result = build_comparison(records)
    if not result.empty:
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
