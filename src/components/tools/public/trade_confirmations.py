"""On-demand all-account IBKR trade confirmations report."""

from collections import defaultdict
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone

from src.utils.connectors.flex_query_api import getFlexQuery
from src.utils.exception import ServiceError


FLEX_QUERY_ID = '1215183'
GROUP_FIELDS = (
    'TradeDate', 'OrderID', 'IsAPIOrder', 'AssetClass', 'Symbol', 'ClientAccountID',
    'AccountAlias', 'Advisor', 'OrderType', 'CurrencyPrimary', 'CommissionCurrency',
)
REQUIRED_FIELDS = set(GROUP_FIELDS) - {'Advisor'} | {'LevelOfDetail', 'Commission', 'Amount'}


def _amount(row, field, index):
    value = row.get(field)
    try:
        result = Decimal(str(value).strip())
    except (InvalidOperation, TypeError, ValueError):
        result = None
    if result is None or not result.is_finite():
        raise ServiceError(
            f'Flex Query {FLEX_QUERY_ID} has an invalid {field} at execution row {index}.',
            status_code=502,
            code='invalid_trade_confirmation',
        )
    return result


def _trade_date(value, index):
    for pattern in ('%Y-%m-%d', '%Y%m%d'):
        try:
            return datetime.strptime(str(value), pattern).date().isoformat()
        except ValueError:
            pass
    raise ServiceError(f'Flex Query has an invalid TradeDate at execution row {index}.', 502)


def read_advisor_mapping(account_ids):
    """Read only the requested accounts and their current assigned advisor."""
    if not account_ids:
        return {}
    from sqlalchemy import text
    from src.utils.connectors.supabase import db

    @db.with_session(commit=False)
    def read(session):
        rows = session.execute(text('''
            SELECT a.ibkr_account_number, v.name
            FROM public.account a
            JOIN public.advisor v ON v.code = a.advisor_code
            WHERE a.ibkr_account_number = ANY(:accounts)
        '''), {'accounts': sorted(account_ids)}).mappings()
        return {r['ibkr_account_number']: r['name'].strip() for r in rows if r['name'] and r['name'].strip()}
    return read()


def group_trade_confirmations(source_rows, advisor_mapping=None):
    if not isinstance(source_rows, list):
        raise ServiceError('Flex Query returned an invalid trade-confirmations dataset.', status_code=502)
    advisor_mapping = advisor_mapping or {}
    grouped = defaultdict(lambda: {'commission': Decimal('0'), 'amount': Decimal('0'), 'count': 0, 'advisor_sources': set()})
    trade_dates = set()
    execution_count = 0
    for index, row in enumerate(source_rows, start=1):
        if not isinstance(row, dict) or 'LevelOfDetail' not in row:
            raise ServiceError(f'Flex Query {FLEX_QUERY_ID} has an incomplete execution row {index}.', status_code=502)
        if row['LevelOfDetail'] != 'EXECUTION':
            continue
        if REQUIRED_FIELDS - set(row) or any(row.get(f) is None or not str(row[f]).strip() for f in ('OrderID', 'ClientAccountID', 'CurrencyPrimary', 'CommissionCurrency')):
            raise ServiceError(f'Flex Query {FLEX_QUERY_ID} has an incomplete execution row {index}.', status_code=502)
        row = dict(row, TradeDate=_trade_date(row['TradeDate'], index))
        provided = str(row.get('Advisor') or '').strip()
        row['Advisor'] = provided or advisor_mapping.get(str(row['ClientAccountID']), '')
        advisor_source = 'IBKR query' if provided else 'AGM current assignment' if row['Advisor'] else 'Unassigned'
        trade_dates.add(row['TradeDate'])
        key = tuple(str(row.get(field) or '') for field in GROUP_FIELDS)
        entry = grouped[key]
        entry['commission'] += _amount(row, 'Commission', index)
        entry['amount'] += abs(_amount(row, 'Amount', index))
        entry['count'] += 1
        entry['advisor_sources'].add(advisor_source)
        execution_count += 1

    rows = []
    for key in sorted(grouped):
        entry = grouped[key]
        rows.append({
            **dict(zip(GROUP_FIELDS, key)),
            'Commission': float(entry['commission']),
            'Amount': float(entry['amount']),
            'ExecutionCount': entry['count'],
            'AdvisorSource': ' / '.join(sorted(entry['advisor_sources'])),
        })
    return {
        'query_id': FLEX_QUERY_ID,
        'trade_dates': sorted(trade_dates),
        'execution_count': execution_count,
        'rows': rows,
        'account_count': len({r['ClientAccountID'] for r in rows}),
        'unassigned_accounts': sorted({r['ClientAccountID'] for r in rows if not r['Advisor']}),
        'retrieved_at': datetime.now(timezone.utc).isoformat(),
    }


def get_trade_confirmations_report():
    source = getFlexQuery(FLEX_QUERY_ID)
    missing = {str(r['ClientAccountID']) for r in source if isinstance(r, dict) and r.get('LevelOfDetail') == 'EXECUTION' and r.get('ClientAccountID') and not r.get('Advisor')} if isinstance(source, list) else set()
    return group_trade_confirmations(source, read_advisor_mapping(missing))
