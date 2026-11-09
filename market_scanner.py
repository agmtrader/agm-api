#!/usr/bin/env python3
"""Run an IBKR IServer market scan using AGM's Trading API connector.

Set the scanner values below to a supported IBKR configuration before running.
The market-data session can interrupt another session for the same IBKR user.
"""
import json
import sys
import time
from pathlib import Path

# The last live parameter catalogue did not advertise individual-bond locations.
# Fill these in only when IBKR provides a supported configuration.
INSTRUMENT = 'BOND'
LOCATION = ''
SCAN_TYPES = ['TOP_PERC_GAIN', 'TOP_PERC_LOSE', 'MOST_ACTIVE', 'MOST_ACTIVE_USD',
              'TOP_TRADE_COUNT', 'TOP_TRADE_RATE', 'SCAN_lipperYieldToMaturity_DESC',
              'HOT_BY_VOLUME', 'TOP_PRICE_RANGE', 'HOT_BY_PRICE_RANGE',
              'TOP_VOLUME_RATE', 'HIGH_STVOLUME_3MIN', 'HIGH_STVOLUME_5MIN',
              'HIGH_STVOLUME_10MIN', 'TOP_OPEN_PERC_GAIN', 'TOP_OPEN_PERC_LOSE',
              'HIGH_VS_13W_HL', 'LOW_VS_13W_HL', 'HIGH_VS_26W_HL', 'LOW_VS_26W_HL',
              'HIGH_VS_52W_HL', 'LOW_VS_52W_HL']
SCAN_TYPES += ['NOT_YET_TRADED_TODAY', 'MOST_ACTIVE_AVG_USD', 'HALTED',
               'TOP_AFTER_HOURS_PERC_GAIN', 'TOP_AFTER_HOURS_PERC_LOSE',
               'HIGH_DIVIDEND_YIELD_IB', 'HIGH_OPT_IMP_VOLAT', 'LOW_OPT_IMP_VOLAT',
               'OPT_VOLUME_MOST_ACTIVE', 'OPT_OPEN_INTEREST_MOST_ACTIVE',
               'SCAN_lipperYieldValue_DESC', 'SCAN_lipperYieldValue_ASC',
               'SCAN_lipperNominalMaturity_DESC', 'SCAN_lipperNominalMaturity_ASC',
               'SCAN_lipperAverageCoupon_DESC', 'SCAN_lipperAverageCoupon_ASC']
FILTERS = []
CATALOG = Path(__file__).with_name('scanner_scan_type_list.json')
OUTPUT = Path(__file__).with_name('bond_scanner_results.json')

def main():

    from src.utils.connectors.ibkr_trading_api import IBKRTradingAPI
    try:
        catalogue = json.loads(CATALOG.read_text())
        scan_types = list(dict.fromkeys(SCAN_TYPES + [item['code'] for item in catalogue if item.get('code')]))
        with IBKRTradingAPI().market_data_session(credential='aguiagm2024') as api:
            previous = json.loads(OUTPUT.read_text()) if OUTPUT.exists() else {}
            contracts = {str(row['con_id']): row for row in previous.get('contracts', []) if row.get('con_id') is not None}
            scans = previous.get('scans', [])
            missing = previous.get('rows_without_con_id', [])
            if scans and scans[-1].get('details', {}).get('ibkr_status') == 401:
                scans.pop()
            completed = {scan['scan_type'] for scan in scans}
            def save():
                OUTPUT.write_text(json.dumps({'scans': scans, 'unique_contracts': len(contracts),
                    'rows_without_con_id': missing, 'planned_scan_types': len(scan_types),
                    'note': 'Results may be capped; BOND sort behavior is unverified.',
                    'contracts': list(contracts.values())}, indent=2, default=str) + '\n')

            for index, scan_type in enumerate(scan_types):
                if scan_type in completed:
                    continue
                if index:
                    time.sleep(1.5)
                try:
                    result = api.run_market_scanner(INSTRUMENT, scan_type, LOCATION, FILTERS)
                except Exception as error:
                    details = getattr(error, 'details', {})
                    scans.append({'scan_type': scan_type, 'error': str(error), 'details': details})
                    save()
                    if details.get('ibkr_status') == 500 and details.get('ibkr_message') == 'Finished: EMPTY response is received.':
                        print(json.dumps(scans[-1]), file=sys.stderr, flush=True)
                        continue
                    break
                if not isinstance(result, dict) or not isinstance(result.get('contracts'), list):
                    raise ValueError('Unexpected scanner response: ' + json.dumps(result, default=str))
                rows = result['contracts']
                before = len(contracts)
                for row in rows:
                    if not isinstance(row, dict) or row.get('con_id') is None:
                        missing.append(row)
                    else:
                        contracts.setdefault(str(row['con_id']), row)
                scans.append({'scan_type': scan_type, 'returned_rows': len(rows),
                              'new_unique_contracts': len(contracts) - before})
                save()
                print(json.dumps(scans[-1]), file=sys.stderr, flush=True)
            print(json.dumps({'scans': scans, 'unique_contracts': len(contracts),
                              'rows_without_con_id': missing,
                              'note': 'Results may be capped; sort behavior for BOND is unverified.',
                              'contracts': list(contracts.values())}, indent=2, default=str))
            if any('error' in scan for scan in scans):
                return 1
    except Exception as error:
        print(f'Scanner failed: {error}', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
