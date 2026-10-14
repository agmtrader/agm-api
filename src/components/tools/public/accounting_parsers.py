"""Deterministic statement adapters. No source writes or model-generated amounts."""
from __future__ import annotations

import csv
import io
import re
import unicodedata
from datetime import datetime
from decimal import Decimal, InvalidOperation


class StatementError(ValueError):
    pass


def money(value, *, blank=False):
    text = str('' if value is None else value).strip()
    if text in {'', '-', 'nan'}:
        if blank:
            return None
        raise StatementError('A required amount is missing.')
    negative = text.startswith('(') and text.endswith(')')
    text = text.replace('$', '').replace(',', '').replace(' ', '').strip('()')
    try:
        amount = Decimal(text)
    except InvalidOperation as exc:
        raise StatementError('A source amount is invalid.') from exc
    if not amount.is_finite():
        raise StatementError('A source amount is not finite.')
    return -abs(amount) if negative else amount


def date_value(value, *, european=False):
    text = str(value).strip()
    formats = ['%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%d.%m.%y', '%d.%m.%Y']
    formats += ['%d/%m/%Y', '%d/%m/%y'] if european else ['%m/%d/%Y', '%m/%d/%y', '%m-%d-%Y', '%m-%d-%y']
    for fmt in formats:
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            pass
    raise StatementError('A transaction date is missing or invalid.')


def text_value(value):
    return unicodedata.normalize('NFKD', str(value or '')).encode('ascii', 'ignore').decode().strip().lower()


def decode_csv(content):
    try:
        return content.decode('utf-8-sig')
    except UnicodeDecodeError:
        return content.decode('latin1')


def reference_in_description(description):
    # Extract labelled source values only; never invent a reference or code.
    match = re.search(r'(?:REF(?:ERENCE)?[:#]\s*|TRANSACTION#:\s*|PAYMENT ID\s+|TRN:\s*)([A-Za-z0-9-]+)', description, re.I)
    return match.group(1) if match else None


def transaction(day, description, amount, row, *, reference=None, code=None, balance=None, posted_date=None):
    return dict(transaction_date=day, posted_date=posted_date or day,
                description=description, amount=amount, debit=max(-amount, Decimal(0)),
                credit=max(amount, Decimal(0)), transaction_reference=reference or reference_in_description(description),
                transaction_code=code or None, running_balance=balance, source_row=row)


def parse_ibkr(content, account, year, month):
    rows = list(csv.reader(io.StringIO(decode_csv(content))))
    identity = next((r[3].strip().split()[0] for r in rows if len(r) > 3 and r[:3] == ['Account Information', 'Data', 'Account'] and r[3].strip()), None)
    if identity != account['source_account']:
        raise StatementError('IBKR statement account does not match the configured account.')
    currency = next((r[3].strip() for r in rows if len(r) > 3 and r[:3] == ['Account Information', 'Data', 'Base Currency']), None)
    if not currency or currency != account['currency']:
        raise StatementError('IBKR base currency is missing or differs from account configuration.')
    header = next((r for r in rows if r[:2] == ['Statement of Funds', 'Header']), None)
    columns = {v.strip(): i for i, v in enumerate(header or [])}
    required = {'Currency', 'Report Date', 'Activity Date', 'Description', 'Debit', 'Credit', 'Balance'}
    if not required.issubset(columns):
        raise StatementError('Statement of Funds columns are missing.')
    tx = []
    opening = closing = None
    active_account = None
    nav = {}
    for index, row in enumerate(rows, 1):
        if len(row) > 3 and row[:3] == ['Account Information', 'Data', 'Account']:
            active_account = row[3].strip().split()[0] if row[3].strip() else None
        if active_account != account['source_account']:
            continue
        if row[:2] == ['Statement of Funds', 'Header']:
            columns = {v.strip(): i for i, v in enumerate(row)}
            if not required.issubset(columns):
                raise StatementError('Statement of Funds columns are missing.')
            continue
        if len(row) > 3 and row[:2] == ['Change in NAV', 'Data'] and row[3].strip():
            nav[row[2].strip()] = money(row[3])
        if row[:2] != ['Statement of Funds', 'Data']:
            continue
        if len(row) <= max(columns[name] for name in required):
            raise StatementError('A Statement of Funds row is truncated.')
        if row[columns['Currency']].strip() != 'Base Currency Summary':
            continue
        description = row[columns['Description']].strip()
        # In IBKR consolidated layouts, balance markers occupy Account while Description is empty.
        if not description and 'Account' in columns and row[columns['Account']].strip() in {'Opening Balance', 'Closing Balance'}:
            description = row[columns['Account']].strip()
        posted = date_value(row[columns['Report Date']])
        if posted[:7] != f'{year}-{month:02d}':
            raise StatementError('IBKR statement period does not match the selected month.')
        if description == 'Opening Balance':
            if opening is not None:
                raise StatementError('Duplicate opening balance.')
            opening = money(row[columns['Balance']])
        elif description == 'Closing Balance':
            if closing is not None:
                raise StatementError('Duplicate closing balance.')
            closing = dict(ending=money(row[columns['Balance']]),
                           debits=abs(money(row[columns['Debit']])), credits=money(row[columns['Credit']]))
        else:
            debit = money(row[columns['Debit']], blank=True) or Decimal(0)
            credit = money(row[columns['Credit']], blank=True) or Decimal(0)
            if debit > 0 or credit < 0:
                raise StatementError('Unexpected IBKR debit or credit sign.')
            if not row[columns['Debit']].strip() and not row[columns['Credit']].strip():
                continue
            activity = row[columns['Activity Date']].strip()
            ref = next((row[columns[k]].strip() for k in ['Transaction ID', 'Reference', 'Transaction Reference'] if k in columns and columns[k] < len(row)), None)
            code = next((row[columns[k]].strip() for k in ['Code', 'Transaction Code'] if k in columns and columns[k] < len(row)), None)
            item = transaction(date_value(activity) if activity else posted, description, debit + credit, index,
                               reference=ref, code=code, balance=money(row[columns['Balance']]), posted_date=posted)
            item['source_account'] = row[columns['Account']].strip() if 'Account' in columns else active_account
            tx.append(item)
    if opening is None or closing is None:
        raise StatementError('Statement of Funds opening/closing totals are missing.')
    return dict(opening=opening, **closing, currency=currency,
                nav_opening=nav.get('Starting Value'), nav_ending=nav.get('Ending Value'), transactions=tx,
                evidence='IBKR CSV Statement of Funds closing-row totals', extraction='native')


def parse_bank_transactions(content, account, extension='csv'):
    if not content:
        raise StatementError('The transaction original is empty.')
    institution = account['institution']
    if extension in {'xls', 'xlsx'}:
        import pandas as pd
        table = pd.read_excel(io.BytesIO(content), header=None).fillna('')
        rows = [[str(x).strip() for x in r] for r in table.values.tolist()]
    else:
        text = decode_csv(content)
        delimiter = ';' if institution == 'BAC Panama' and ';' in text.splitlines()[0] else ','
        rows = list(csv.reader(io.StringIO(text), delimiter=delimiter))
    if not rows:
        raise StatementError('The transaction export is empty; its coverage cannot be established.')
    if institution == 'BAC Panama' and extension in {'xls', 'xlsx'}:
        product = next((r[1] for r in rows if len(r) > 1 and text_value(r[0]) == 'producto'), None)
        if product != account['source_account']:
            raise StatementError('BAC workbook account does not match configuration.')
    if institution == 'BAC Panama' and extension == 'csv' and len(rows[0]) > 20 and rows[0][0].strip().isdigit():
        output = []
        for i, row in enumerate(rows, 1):
            if len(row) <= 21 or row[1].strip() != account['source_account']:
                raise StatementError('BAC transaction account or row structure is invalid.')
            item = transaction(date_value(row[3], european=True), row[18].strip(), money(row[10]), i,
                               code=row[6].strip() or None)
            # This unlabelled record identifier is shared by different movements;
            # it must not be presented as a verified transaction reference.
            item['source_export_identifier'] = row[16].strip() or None
            output.append(item)
        return output
    specs = {
        'Banesco': ('Post Date', 'Description', None, 'Debit', 'Credit', 'Check', None, 'Balance'),
        'Terrabank': ('date', 'description', 'amount', None, None, None, None, 'balance'),
        'PNC BANK': ('Transaction Date', 'Transaction Description', 'Amount', None, None, None, None, None),
        'Truist': ('Posted Date', 'Description', 'Amount', None, None, 'Check/Serial #', 'Transaction Type', 'Daily Posted Balance'),
        'Chase': ('Posting Date', 'Description', 'Amount', None, None, 'Check or Slip #', 'Type', 'Balance'),
        'Mercury': ('Date', 'Description', 'Amount', None, None, 'Reference', 'Type', None),
        'BAC Panama': ('Fecha', 'Descripción', None, 'Débitos', 'Créditos', 'Referencia', 'Código', 'Balance*'),
    }
    if institution not in specs:
        raise StatementError('Unsupported institution.')
    if institution == 'Terrabank' and any({'trntype', 'dtposted', 'tranamt', 'fitid'}.issubset({text_value(v) for v in row}) for row in rows[:10]):
        specs['Terrabank'] = ('DTPOSTED', 'MEMO', 'TRANAMT', None, None, 'FITID', 'TRNTYPE', 'RUNNINGBAL')
    day, description, amount, debit, credit, reference, code, balance = specs[institution]
    needed = {text_value(x) for x in (day, description, amount, debit, credit) if x}
    header_index = next((i for i, r in enumerate(rows) if needed.issubset({text_value(c) for c in r})), None)
    if header_index is None:
        raise StatementError('The transaction export format is not supported; required headers are missing.')
    columns = {text_value(v): i for i, v in enumerate(rows[header_index]) if v.strip()}
    output = []
    for i, row in enumerate(rows[header_index + 1:], header_index + 2):
        if institution == 'BAC Panama' and row and text_value(row[0]) == 'cuadro de resumen':
            break
        def field(name):
            pos = columns.get(text_value(name)) if name else None
            return row[pos].strip() if pos is not None and pos < len(row) else ''
        if not any(v.strip() for v in row):
            continue
        raw_day = field(day)
        if not raw_day and institution == 'BAC Panama' and text_value(field(description)) in {'saldo inicial', 'saldo final', 'totales', 'total'}:
            continue
        if not raw_day:
            raise StatementError('A transaction row has no date.')
        if institution == 'Banesco' and field('Account Number').lstrip('0') not in {account['source_account'].lstrip('0'), account['suffix'].lstrip('0')}:
            raise StatementError('Banesco transaction account does not match configuration.')
        if amount:
            signed = money(field(amount))
        else:
            d = money(field(debit), blank=True) or Decimal(0)
            c = money(field(credit), blank=True) or Decimal(0)
            if d < 0 or c < 0:
                raise StatementError('Unexpected bank debit/credit sign.')
            signed = c - d
        posted = date_value(raw_day, european=institution == 'BAC Panama')
        activity = field('Transaction Date') if institution == 'Truist' else ''
        output.append(transaction(date_value(activity) if activity else posted, field(description), signed, i,
                                  reference=field(reference) or None, code=field(code) or None,
                                  balance=money(field(balance), blank=True), posted_date=posted))
    return output


MONEY_TOKEN = r'(?<![\d.,])\(?[-+]?\s*\$?\s*(?:\d[\d,]*\.\d{2}|\.\d{2})\)?(?![\d.])'


def _line_amount(text, label, *, optional=False):
    match = re.search(label + r'[^\r\n]*?(' + MONEY_TOKEN + ')', text, re.I)
    if not match:
        if optional:
            return None
        raise StatementError('A required statement summary field is missing: ' + label)
    return money(match.group(1))


def parse_bank_summary(text, account, year, month):
    institution = account['institution']
    normalized = text.replace('\r', '')
    normalizations = []
    if institution == 'Terrabank':
        # The legacy scanned fee prints a doubled decimal point (50..00).
        # Retain that fragment; canonicalize punctuation only, never digits.
        pattern = r'(Cycle Service Charge[^\n]*?)(\d[\d,]*\.\.\d{2})(?![\d.])'
        def canonical_fee(match):
            source = match.group(2)
            canonical = source.replace('..', '.')
            normalizations.append(dict(field='Cycle Service Charge', source_text=source, parsed_text=canonical))
            return match.group(1) + canonical
        normalized = re.sub(pattern, canonical_fee, normalized, flags=re.I)
    # Account identities are validated in the summary, never inferred from transaction descriptions.
    identity_patterns = {
        'Banesco': r'ACCOUNT NUMBER\s+(\d+)',
        'Chase': r'Account Number:\s*([\d]+)',
        'PNC BANK': r'Account [Nn]umber:\s*([Xx\d*-]+)',
        'Truist': r'TRUIST SIMPLE BUSINESS CHECKING\s+(\d+)',
        'Mercury': r'Account Number\s+(\d+)',
        'Terrabank': r'Account (?:Number:|#:)\s*([Xx*\d]+)',
        'BAC Panama': r'DETALLE DE CUENTA\s+(\d+)',
    }
    identity_match = re.search(identity_patterns[institution], normalized, re.I)
    if not identity_match:
        raise StatementError('Statement account identity could not be read.')
    identity = identity_match.group(1)
    if re.search('[Xx*]', identity):
        valid = identity.endswith(account['suffix'])
    else:
        valid = re.sub(r'\D', '', identity).lstrip('0') == re.sub(r'\D', '', account['source_account']).lstrip('0')
        if account['source_account'] == account['suffix']:
            valid = identity.endswith(account['suffix'])
    if not valid:
        raise StatementError('Statement account identity does not match configuration.')
    # Verify a statement date in the summary, not a filename alone.
    month_name = datetime(year, month, 1).strftime('%B')
    month_abbr = month_name[:3]
    dates = [rf'{month_name}\s+\d{{1,2}},?\s+{year}', rf'{month:02d}/\d{{2}}/{year}',
             rf'{month_abbr}/\d{{2}}/{str(year)[-2:]}', rf'{year}-{month:02d}']
    if institution == 'BAC Panama':
        spanish = ['ENE', 'FEB', 'MAR', 'ABR', 'MAY', 'JUN', 'JUL', 'AGO', 'SEP', 'OCT', 'NOV', 'DIC'][month - 1]
        dates.append(rf'{spanish}/\d{{2}}/{str(year)[-2:]}')
    date_patterns = {
        'Banesco': r'Ending balance on ([^\n$]+)' if 'Ending balance on' in normalized else r'Date\s+(\d{1,2}/\d{1,2}/\d{2})',
        'Terrabank': r'Statement Date\s+([^\n]+)' if 'Statement Date' in normalized else r'Ending balance on ([^\n$]+)',
        'Truist': r'Your new balance as of ([^\n=$]+)',
        'PNC BANK': r'For the Period[^\n]+ to ([^\n]+)',
        'Chase': r'through ([^\n]+)',
        'Mercury': r'([^\n]+) statement',
        'BAC Panama': r'([A-Z]{3}/\d{2}/\d{2})',
    }
    dated = re.search(date_patterns[institution], normalized, re.I)
    dates.append(rf'{month_name}\s+{year}')
    dates.append(rf'0?{month}/\d{{1,2}}/{str(year)[-2:]}')
    if not dated or not any(re.search(pattern, dated.group(1), re.I) for pattern in dates):
        raise StatementError('Statement period could not be verified against the selected month.')
    if institution == 'PNC BANK':
        match = re.search(r'Balance Summary\s+Beginning\s+balance\s+Deposits and\s+other additions\s+Checks and other\s+deductions\s+Ending\s+balance\s+(' + MONEY_TOKEN + r')\s+(' + MONEY_TOKEN + r')\s+(' + MONEY_TOKEN + r')\s+(' + MONEY_TOKEN + ')', normalized, re.I)
        if not match:
            raise StatementError('PNC balance-summary layout is unsupported.')
        opening, credits, debits, ending = [money(v) for v in match.groups()]
    elif institution == 'BAC Panama':
        # The detail's Saldo al Corte credit column includes opening cash. Use the independent summary table.
        match = re.search(r'(?:^|\n)' + re.escape(account['source_account']) + r'\s+\d+\s+(' + MONEY_TOKEN + r')\s+\d+\s+(' + MONEY_TOKEN + r')\s+(' + MONEY_TOKEN + r')\s+(' + MONEY_TOKEN + r')\s+(' + MONEY_TOKEN + ')', normalized)
        if not match:
            raise StatementError('BAC independent account-summary totals are missing.')
        debits, credits, average, opening, ending = [money(v) for v in match.groups()]
    elif institution == 'Truist':
        opening = _line_amount(normalized, 'Your previous balance')
        ending = _line_amount(normalized, 'Your new balance')
        credits = _line_amount(normalized, 'Deposits, credits and interest')
        debits = abs(_line_amount(normalized, 'Other withdrawals, debits and service charges')) + abs(_line_amount(normalized, r'\nChecks'))
    elif institution == 'Terrabank':
        opening = _line_amount(normalized, 'Beginning Balance')
        ending = _line_amount(normalized, 'Ending Balance')
        credits = _line_amount(normalized, 'Deposits and Credits')
        if 'Total Checks and Debits' in normalized:
            debits = abs(_line_amount(normalized, 'Total Checks and Debits')) + abs(_line_amount(normalized, 'Cycle Service Charge'))
        else:
            debits = abs(_line_amount(normalized, 'Withdrawals and Debits'))
    elif institution == 'Mercury':
        opening = _line_amount(normalized, 'Beginning Balance')
        ending = _line_amount(normalized, 'Statement balance')
        credits = _line_amount(normalized, 'Total deposits')
        debits = abs(_line_amount(normalized, 'Total withdrawals'))
    elif institution == 'Banesco':
        if 'Previous Balance' in normalized:
            opening = _line_amount(normalized, 'Previous Balance')
            ending = _line_amount(normalized, 'Current Balance')
            credits = _line_amount(normalized, 'Deposits/Credits')
            debits = abs(_line_amount(normalized, 'Checks/Debits')) + abs(_line_amount(normalized, 'Total Fees'))
        else:
            opening = _line_amount(normalized, 'Beginning balance on')
            ending = _line_amount(normalized, 'Ending balance on')
            credits = _line_amount(normalized, 'Deposits and other credits')
            debits = abs(_line_amount(normalized, 'Checks and withdrawals')) + abs(_line_amount(normalized, 'Service fees'))
    else:  # Chase categories appear only when present; the block itself must be identified.
        match = re.search(r'CHECKING SUMMARY(.*?)Ending Balance[^\n]*', normalized, re.S)
        if not match:
            raise StatementError('Chase checking summary is missing.')
        block = match.group(0)
        opening = _line_amount(block, 'Beginning Balance')
        ending = _line_amount(block, 'Ending Balance')
        credits = _line_amount(block, 'Deposits and Additions', optional=True) or Decimal(0)
        debits = sum((abs(_line_amount(block, label, optional=True) or Decimal(0)) for label in ['Checks Paid', 'ATM & Debit Card Withdrawals', 'Electronic Withdrawals', 'Other Withdrawals', r'\nFees']), Decimal(0))
    if credits < 0 or debits < 0:
        raise StatementError('Statement credit/debit totals have invalid signs.')
    return dict(opening=opening, ending=ending, credits=credits, debits=debits,
                currency=account['currency'], evidence=institution + ' PDF account summary',
                normalizations=normalizations)


def reconcile(summary, transactions, *, tolerance=Decimal('0.01')):
    """Independent statement totals, transaction totals, and balance equations."""
    credits = sum((t['credit'] for t in transactions), Decimal(0))
    debits = sum((t['debit'] for t in transactions), Decimal(0))
    differences = dict(
        debit_total=debits - summary['debits'], credit_total=credits - summary['credits'],
        statement_balance=summary['opening'] + summary['credits'] - summary['debits'] - summary['ending'],
        transaction_balance=summary['opening'] + credits - debits - summary['ending'],
    )
    # Validate running balances only when the source exports them in ledger order.
    return dict(transaction_credits=credits, transaction_debits=debits,
                computed_ending=summary['opening'] + credits - debits,
                differences=differences, tolerance=tolerance,
                reconciled=all(abs(value) < tolerance for value in differences.values()))
