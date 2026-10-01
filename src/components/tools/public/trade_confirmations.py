"""On-demand all-account IBKR trade confirmations report."""

from collections import defaultdict
from decimal import Decimal, InvalidOperation

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


def group_trade_confirmations(source_rows):
    if not isinstance(source_rows, list):
        raise ServiceError('Flex Query returned an invalid trade-confirmations dataset.', status_code=502)
    if not source_rows:
        return {'query_id': FLEX_QUERY_ID, 'trade_dates': [], 'execution_count': 0, 'rows': []}

    missing = REQUIRED_FIELDS - set(source_rows[0])
    if missing:
        raise ServiceError(
            f'Flex Query {FLEX_QUERY_ID} is missing required columns: {", ".join(sorted(missing))}.',
            status_code=502,
            code='invalid_trade_confirmation',
        )

    grouped = defaultdict(lambda: {'commission': Decimal('0'), 'amount': Decimal('0'), 'count': 0})
    trade_dates = set()
    execution_count = 0
    for index, row in enumerate(source_rows, start=1):
        if not isinstance(row, dict) or REQUIRED_FIELDS - set(row):
            raise ServiceError(f'Flex Query {FLEX_QUERY_ID} has an incomplete execution row {index}.', status_code=502)
        if row['LevelOfDetail'] != 'EXECUTION':
            continue
        if not row['TradeDate']:
            raise ServiceError(f'Flex Query {FLEX_QUERY_ID} has no TradeDate at execution row {index}.', status_code=502)
        trade_dates.add(str(row['TradeDate']))
        key = tuple(str(row.get(field) or '') for field in GROUP_FIELDS)
        entry = grouped[key]
        entry['commission'] += _amount(row, 'Commission', index)
        entry['amount'] += abs(_amount(row, 'Amount', index))
        entry['count'] += 1
        execution_count += 1

    rows = []
    for key in sorted(grouped):
        entry = grouped[key]
        rows.append({
            **dict(zip(GROUP_FIELDS, key)),
            'Commission': float(entry['commission']),
            'Amount': float(entry['amount']),
            'ExecutionCount': entry['count'],
        })
    return {
        'query_id': FLEX_QUERY_ID,
        'trade_dates': sorted(trade_dates),
        'execution_count': execution_count,
        'rows': rows,
    }


def get_trade_confirmations_report():
    return group_trade_confirmations(getFlexQuery(FLEX_QUERY_ID))
