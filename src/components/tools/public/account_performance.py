"""Read-only account funding overview from the existing IBKR Drive backups.

Cash-adjusted NAV change includes realized/unrealized valuation effects, income,
fees and FX. Securities transfers are not covered by these two source queries;
the result is provisional, never a certified investment return.
"""
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import calendar
import csv
import io
import re
from zoneinfo import ZoneInfo

from src.utils.connectors.drive import GoogleDrive
from src.utils.exception import ServiceError
from src.utils.lazy_client import LazyClient
from src.utils.report_cache import ReportCache


NAV_FOLDER = '1WgYA-Q9mnPYrbbLfYLuJZwUIWBYjiD4c'
FLOW_FOLDER = '1ZCJfH2hxvMLuP470HMa-D33_R_l-Lhtx'
Drive = LazyClient(GoogleDrive)
_catalog_cache = ReportCache(30, keys=('nav', 'flows'))
TRANSFER_LIMITATION = (
    'Securities transfers are not covered by these sources. Cash-adjusted change '
    'is provisional and may include transferred securities; it is not confirmed '
    'investment P&L or a percentage return.'
)


def _date(value):
    text = str(value or '').strip()
    for fmt in ('%Y-%m-%d', '%Y%m%d', '%m/%d/%Y', '%Y%m%d;%H%M%S', '%Y-%m-%d %H:%M:%S'):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            pass
    raise ValueError('Invalid date')


def _number(value):
    if isinstance(value, bool) or value is None:
        raise ValueError('Invalid number')
    try:
        result = Decimal(str(value).strip())
    except InvalidOperation as exc:
        raise ValueError('Invalid number') from exc
    if not result.is_finite():
        raise ValueError('Invalid number')
    return result


def _money(value):
    return float(value.quantize(Decimal('.01'))) if value is not None else None


def _catalog(folder):
    files = Drive.get_files_in_folder(folder)
    if not isinstance(files, list):
        raise ServiceError('The report source inventory is unavailable.', 502)
    result = [f for f in files if f.get('mimeType') != 'application/vnd.google-apps.folder']
    for child in files:
        if child.get('mimeType') == 'application/vnd.google-apps.folder' and re.fullmatch(r'\d{4}', child.get('name', '')):
            children = Drive.get_files_in_folder(child['id'])
            if not isinstance(children, list):
                raise ServiceError('The report source inventory is unavailable.', 502)
            result.extend(children)
    return result


def _source(file, start, end):
    return {'id': file['id'], 'name': file['name'], 'start': start.isoformat(), 'end': end.isoformat()}


def select_sources(nav_files, flow_files, start, end):
    """Select dated sources; partial-month backups cannot certify a full month."""
    nav_by_date = {}
    for file in nav_files:
        match = re.fullmatch(r'734782_(\d{8})\.csv', file.get('name', ''), re.I)
        if not match:
            continue
        try:
            day = _date(match[1])
        except ValueError:
            continue
        nav_by_date.setdefault(day, {})[file['id']] = file

    def nav_at(target):
        candidates = [d for d in nav_by_date if d <= target]
        if not candidates:
            return None
        day = max(candidates)
        if len(nav_by_date[day]) != 1:
            raise ServiceError('Duplicate NAV backups for the selected boundary.', 502)
        return _source(next(iter(nav_by_date[day].values())), day, day)

    opening = nav_at(start - timedelta(days=1))
    closing = nav_at(end)
    if closing is None or _date(closing['end']) < start:
        raise ServiceError('No closing NAV snapshot is available in the selected period.', 422, code='nav_history_unavailable')
    # Include flows after the actual opening snapshot, including weekend gaps.
    opening_is_recent = opening and (start - timedelta(days=1) - _date(opening['end'])).days <= 7
    effective_start = _date(opening['end']) + timedelta(days=1) if opening_is_recent else start
    effective_end = _date(closing['end'])
    flow_candidates = []
    for file in flow_files:
        match = re.fullmatch(r'794867_(\d{8})_(\d{8})\.csv', file.get('name', ''), re.I)
        if not match:
            continue
        try:
            first, last = _date(match[1]), _date(match[2])
        except ValueError:
            continue
        if first <= last and (first.year, first.month) == (last.year, last.month):
            flow_candidates.append((first, last, file))
    cursor = effective_start.replace(day=1)
    flows, missing = [], []
    while cursor <= effective_end:
        month_end = cursor.replace(day=calendar.monthrange(cursor.year, cursor.month)[1])
        lower, upper = max(cursor, effective_start), min(month_end, effective_end)
        matches = [(first, last, file) for first, last, file in flow_candidates if first <= lower and last >= upper]
        if not matches:
            missing.append(cursor.strftime('%Y-%m'))
        else:
            # Prefer the widest/latest verified source, never listing order.
            best = max((last, -first.toordinal()) for first, last, _ in matches)
            winners = {f['id']: (first, last, f) for first, last, f in matches if (last, -first.toordinal()) == best}
            if len(winners) != 1:
                raise ServiceError('Duplicate cash-flow backups for the selected month.', 502)
            first, last, file = next(iter(winners.values()))
            flows.append(_source(file, first, last))
        cursor = month_end + timedelta(days=1)
    return {'opening': opening, 'closing': closing, 'flows': flows, 'missing_flow_months': missing,
            'effective_start': effective_start.isoformat(), 'effective_end': effective_end.isoformat()}


def _nav_rows(rows, source):
    if source is None:
        return {}
    if not rows:
        raise ServiceError('A selected NAV snapshot contains no account values.', 502)
    grouped = {}
    for row in rows:
        account = str(row.get('ClientAccountID') or '').strip()
        if not account:
            raise ServiceError('A NAV source row has no account identifier.', 502)
        grouped.setdefault(account, []).append(row)
    result = {}
    for account, values in grouped.items():
        whole_account = [r for r in values if not str(r.get('Model') or '').strip()]
        issues = []
        selected = whole_account[0] if len(whole_account) == 1 else values[0]
        if len(whole_account) != 1:
            issues.append('ambiguous_account_nav')
        currency = str(selected.get('CurrencyPrimary') or '').strip().upper()
        if not re.fullmatch(r'[A-Z]{3}', currency):
            issues.append('missing_base_currency')
        total = None
        try:
            if _date(selected.get('ReportDate')).isoformat() != source['end']:
                raise ValueError('Snapshot date mismatch')
            if not issues:
                total = _number(selected.get('Total'))
        except ValueError:
            issues.append('invalid_nav_value_or_date')
        result[account] = {'value': total, 'currency': currency, 'alias': str(selected.get('AccountAlias') or account), 'issues': issues}
    return result


def build_report(sources, source_rows, start, end):
    """Pure normalization/calculation boundary; never infer missing money as zero."""
    opening = _nav_rows(source_rows.get(sources['opening']['id'], []) if sources['opening'] else [], sources['opening'])
    closing = _nav_rows(source_rows[sources['closing']['id']], sources['closing'])
    first, last = _date(sources['effective_start']), _date(sources['effective_end'])
    flow_accounts = {}
    for source in sources['flows']:
        for row in source_rows[source['id']]:
            account = str(row.get('ClientAccountID') or '').strip()
            if not account:
                raise ServiceError('A cash-flow source row has no account identifier.', 502)
            try:
                day = _date(row.get('Date/Time'))
            except ValueError as exc:
                raise ServiceError('A cash-flow source row has an invalid transaction date.', 502) from exc
            if first <= day <= last:
                flow_accounts.setdefault(account, []).append(row)

    rows = []
    for account in sorted(opening.keys() | closing.keys() | flow_accounts.keys()):
        before, after = opening.get(account, {}), closing.get(account, {})
        issues = list(dict.fromkeys(before.get('issues', []) + after.get('issues', [])))
        currency = after.get('currency') or before.get('currency') or ''
        if before.get('value') is None:
            issues.append('missing_opening_value')
        if after.get('value') is None:
            issues.append('missing_closing_value')
        if before.get('currency') and after.get('currency') and before['currency'] != after['currency']:
            issues.append('base_currency_changed')
        if sources['opening'] and (start - timedelta(days=1) - _date(sources['opening']['end'])).days > 7:
            issues.append('stale_opening_snapshot')
        if (end - last).days > 7:
            issues.append('stale_closing_snapshot')
        if sources['missing_flow_months']:
            issues.append('cash_flow_coverage_incomplete')
        deposits, withdrawals = Decimal(0), Decimal(0)
        seen = {}
        flow_issues = []
        for flow in flow_accounts.get(account, []):
            try:
                if str(flow.get('Type') or '').strip() != 'Deposits/Withdrawals' or str(flow.get('Model') or '').strip() or str(flow.get('LevelOfDetail') or '').strip() != 'DETAIL':
                    raise ValueError('Unsupported cash-flow category/model')
                amount = _number(flow.get('Amount'))
                flow_currency = str(flow.get('CurrencyPrimary') or '').strip().upper()
                if not re.fullmatch(r'[A-Z]{3}', currency) or not re.fullmatch(r'[A-Z]{3}', flow_currency):
                    raise ValueError('Currency unavailable')
                rate = Decimal(1) if flow_currency == currency else _number(flow.get('FXRateToBase'))
                if flow_currency == currency and flow.get('FXRateToBase') not in (None, '') and _number(flow['FXRateToBase']) != 1:
                    raise ValueError('Same-currency FX rate conflicts with account base currency')
                if rate <= 0:
                    raise ValueError('Invalid FX rate')
                transaction_id = str(flow.get('TransactionID') or '').strip()
                if not transaction_id:
                    raise ValueError('Transaction ID unavailable')
                signature = (_date(flow['Date/Time']), amount, flow_currency, rate)
                if transaction_id in seen:
                    if seen[transaction_id] != signature:
                        flow_issues.append('conflicting_cash_flow')
                    continue
                seen[transaction_id] = signature
                converted = amount * rate
                if converted >= 0:
                    deposits += converted
                else:
                    withdrawals -= converted
            except ValueError:
                flow_issues.append('invalid_cash_flow')
        issues = list(dict.fromkeys(issues + flow_issues))
        flows_available = not sources['missing_flow_months'] and not flow_issues and bool(re.fullmatch(r'[A-Z]{3}', currency)) and 'base_currency_changed' not in issues
        change = after['value'] - before['value'] - deposits + withdrawals if not issues else None
        rows.append({'account': account, 'alias': after.get('alias') or before.get('alias') or account,
                     'currency': currency or None, 'opening_currency': before.get('currency') or None,
                     'opening_value': _money(before.get('value')), 'closing_value': _money(after.get('value')),
                     'deposits': _money(deposits) if flows_available else None,
                     'withdrawals': _money(withdrawals) if flows_available else None,
                     'net_contributions': _money(deposits - withdrawals) if flows_available else None,
                     'cash_adjusted_change': _money(change), 'issues': issues,
                     'status': 'partial' if issues else 'provisional',
                     'transaction_count': len(seen)})
    return {'requested_start': start.isoformat(), 'requested_end': end.isoformat(),
            'covered_start': first.isoformat(), 'covered_end': last.isoformat(),
            'opening_date': sources['opening']['end'] if sources['opening'] else None,
            'sources': sources, 'rows': rows, 'limitations': [TRANSFER_LIMITATION],
            'retrieved_at': datetime.now(timezone.utc).isoformat()}


def get_account_performance(start_date=None, end_date=None):
    today = datetime.now(ZoneInfo('America/Costa_Rica')).date()
    try:
        if any(value and not re.fullmatch(r'\d{4}-\d{2}-\d{2}', value) for value in (start_date, end_date)):
            raise ValueError('Invalid date format')
        start = date.fromisoformat(start_date) if start_date else date(today.year, 1, 1)
        end = date.fromisoformat(end_date) if end_date else today
        if start <= date.min or start > end or end > today:
            raise ValueError('Invalid period')
    except (ValueError, TypeError) as exc:
        raise ServiceError('Use valid YYYY-MM-DD dates, with start on/before end and no future dates.', 400) from exc
    nav_files = _catalog_cache.get('nav', lambda: _catalog(NAV_FOLDER))
    flow_files = _catalog_cache.get('flows', lambda: _catalog(FLOW_FOLDER))
    sources = select_sources(nav_files, flow_files, start, end)
    selected = {s['id']: s for s in [sources['opening'], sources['closing'], *sources['flows']] if s}

    def read(source):
        # Preserve transaction IDs and decimal amounts; pandas' shared parser
        # can coerce identifiers/amounts to floating point before validation.
        content = Drive.download_file(file_id=source['id'], parse=False)
        if not isinstance(content, bytes):
            raise ServiceError('A report source returned invalid data.', 502)
        try:
            text = content.decode('utf-8-sig')
        except UnicodeDecodeError:
            text = content.decode('latin1')
        reader = csv.DictReader(io.StringIO(text))
        required = {'ClientAccountID', 'Model', 'CurrencyPrimary'}
        required |= {'ReportDate', 'Total'} if source['name'].startswith('734782_') else {'FXRateToBase', 'Date/Time', 'Amount', 'Type', 'TransactionID', 'LevelOfDetail'}
        if not reader.fieldnames or not required.issubset(reader.fieldnames) or len(reader.fieldnames) != len(set(reader.fieldnames)):
            raise ServiceError('A report source is missing required columns or has duplicate headers.', 502)
        rows = list(reader)
        if any(None in row for row in rows):
            raise ServiceError('A report source contains malformed rows.', 502)
        return source['id'], rows

    with ThreadPoolExecutor(max_workers=4) as pool:
        source_rows = dict(pool.map(read, selected.values()))
    return build_report(sources, source_rows, start, end)
