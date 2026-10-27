"""Account/month controls over preserved Drive originals.

Account edits are immutable Drive revisions. Conflicting concurrent edits fail
closed instead of silently choosing a revision and losing another user's changes.
"""
from __future__ import annotations

import base64
import binascii
import calendar
import copy
import hashlib
import io
import json
import re
import subprocess
import sys
import threading
import time
import uuid
import zipfile
from collections import OrderedDict
from concurrent.futures import Future
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

from src.utils.connectors.drive import GoogleDrive
from src.utils.exception import ServiceError
from src.utils.lazy_client import LazyClient
from .accounting_parsers import StatementError, parse_ibkr, parse_bank_summary, parse_bank_transactions, reconcile


Drive = LazyClient(GoogleDrive)
BANK_ROOT = '1HvteU_DbER4ERZVBpgDB_3Vz1CzBc5Pb'
IBKR_ROOT = '1zj98lJaIKQoVIJY0ThexTUXRGI8vAeFY'
ROOTS = {BANK_ROOT, IBKR_ROOT}
FOLDER_MIME = 'application/vnd.google-apps.folder'
SEED = Path(__file__).parents[3] / 'config/accounting_accounts.json'
MAX_FILE = 25 * 1024 * 1024
MAX_BUNDLE = 128 * 1024 * 1024
_cache = OrderedDict()
_cache_lock = threading.Lock()
_registry_lock = threading.Lock()
_analysis_slots = threading.BoundedSemaphore(4)
_cache_epoch = 0
_inflight = {}


def _cached(key, load, ttl=30):
    now = time.monotonic()
    with _cache_lock:
        epoch = _cache_epoch
        item = _cache.get(key)
        if item and item[0] > now:
            _cache.move_to_end(key)
            return copy.deepcopy(item[1])
        flight_key = (epoch, key)
        existing = _inflight.get(flight_key)
        future = existing or Future()
        if existing is None:
            _inflight[flight_key] = future
    if existing is not None:
        return copy.deepcopy(future.result())
    try:
        value = load()
        with _cache_lock:
            if epoch == _cache_epoch:
                # A slow Drive read must not expire before it finishes.
                _cache[key] = (time.monotonic() + ttl, copy.deepcopy(value))
                while len(_cache) > 96 or sum(len(v[1]) for v in _cache.values() if isinstance(v[1], bytes)) > 32 * 1024 * 1024:
                    _cache.popitem(last=False)
        future.set_result(copy.deepcopy(value))
        return value
    except BaseException as exc:
        future.set_exception(exc)
        raise
    finally:
        with _cache_lock:
            _inflight.pop(flight_key, None)


def clear_cache():
    global _cache_epoch
    with _cache_lock:
        _cache_epoch += 1
        _cache.clear()


def _json_numbers(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return {k: _json_numbers(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_numbers(v) for v in value]
    return value


def period(year, month=None):
    try:
        y = int(str(year))
        m = int(str(month)) if month is not None else None
    except (ValueError, TypeError):
        raise ServiceError('Select a valid statement year and month.', 400) from None
    today = datetime.now(ZoneInfo('America/Costa_Rica')).date()
    if not 2000 <= y <= today.year or m is not None and not 1 <= m <= 12:
        raise ServiceError('Select a valid statement year and month.', 400)
    return y, m


def load_registry(*, fresh=False):
    files = Drive.get_files_in_folder(BANK_ROOT) if fresh else _cached(('registry_files', BANK_ROOT), lambda: Drive.get_files_in_folder(BANK_ROOT), ttl=10)
    revisions = [f for f in files if re.fullmatch(r'agm-accounting-registry-[a-f0-9]{32}\.json', f['name'])]
    if len(revisions) > 200:
        raise ServiceError('Accounting account history needs review before further edits.', 409)
    snapshots = []
    for f in revisions:
        data = _cached(('registry', f['id'], f.get('modifiedTime')), lambda f=f: json.loads(Drive.download_file(f['id'], parse=False)), ttl=300)
        if f['name'] != f"agm-accounting-registry-{data.get('revision')}.json":
            raise ServiceError('Accounting account history is invalid.', 409)
        snapshots.append(data)
    parents = {s.get('parent_revision') for s in snapshots}
    tips = [s for s in snapshots if s['revision'] not in parents]
    if snapshots and (len(tips) != 1 or any(s.get('parent_revision') not in {'seed', *(x['revision'] for x in snapshots)} for s in snapshots)):
        raise ServiceError('Concurrent account edits conflict. Accounting account history requires review; no revision was silently discarded.', 409)
    result = tips[0] if tips else {'revision': 'seed', 'accounts': json.loads(SEED.read_text())}
    validate_accounts(result['accounts'])
    return result


def validate_accounts(accounts):
    institutions = {'IBKR', 'Chase', 'Terrabank', 'Mercury', 'Truist', 'BAC Panama', 'Banesco', 'PNC BANK'}
    if not isinstance(accounts, list) or not 1 <= len(accounts) <= 100:
        raise ServiceError('The accounting account list is invalid.', 400)
    identities = set()
    for a in accounts:
        if not isinstance(a, dict) or not re.fullmatch(r'[A-Za-z0-9-]{1,40}', str(a.get('account', ''))) or a['account'] in identities:
            raise ServiceError('Account identifiers must be unique and contain only letters, numbers or hyphens.', 400)
        identities.add(a['account'])
        if a.get('institution') not in institutions or a.get('status') not in {'open', 'closed'}:
            raise ServiceError('Select a supported institution and account status.', 400)
        for key in ['opened_on', 'closed_on']:
            if a.get(key):
                try:
                    date.fromisoformat(a[key])
                except (ValueError, TypeError):
                    raise ServiceError('Account dates must use YYYY-MM-DD.', 400) from None
        if a.get('opened_on') and a.get('closed_on') and a['closed_on'] < a['opened_on']:
            raise ServiceError('Closing date cannot precede opening date.', 400)
        if a['status'] == 'open' and a.get('closed_on'):
            raise ServiceError('An open account cannot have a closing date.', 400)
        if 'ignore_unknown_closing_date' in a and not isinstance(a['ignore_unknown_closing_date'], bool):
            raise ServiceError('The unknown closing-date policy must be true or false.', 400)
        if not re.fullmatch(r'[A-Z]{3}', str(a.get('currency', ''))) or not re.fullmatch(r'[A-Za-z0-9-]{1,40}', str(a.get('source_account', ''))) or not re.fullmatch(r'\d{4}', str(a.get('suffix', ''))):
            raise ServiceError('Account currency, source identifier and four-digit suffix are required.', 400)
        if a['institution'] != 'IBKR' and a['currency'] != 'USD':
            raise ServiceError('The supported bank statement formats are USD. Other currencies require a verified adapter.', 400)
        if not isinstance(a.get('owner'), str) or not a['owner'].strip() or len(a['owner']) > 250:
            raise ServiceError('The accounting account owner is required.', 400)
        if a.get('required_formats') != ['pdf', 'csv']:
            raise ServiceError('Cris requires monthly PDF and raw CSV backups.', 400)
        selections = a.get('source_selections', {})
        additional = a.get('additional_folder_ids', [])
        if not isinstance(additional, list) or len(additional) > 10 or any(not isinstance(f, str) or not re.fullmatch(r'[A-Za-z0-9_-]{10,100}', f) for f in additional):
            raise ServiceError('Additional historical folders are invalid.', 400)
        if not isinstance(selections, dict) or len(selections) > 600:
            raise ServiceError('Source selections are invalid.', 400)
        for key, chosen in selections.items():
            if not re.fullmatch(r'\d{4}-(?:0[1-9]|1[0-2])', key) or not isinstance(chosen, dict) or set(chosen) - {'pdf', 'raw'} or any(not isinstance(v, str) or len(v) > 500 for v in chosen.values()):
                raise ServiceError('Source selections are invalid.', 400)


def _managed_folder(folder):
    for _ in range(12):
        if folder in ROOTS:
            return True
        if not re.fullmatch(r'[A-Za-z0-9_-]{10,100}', str(folder)):
            return False
        info = Drive.get_file_info_by_id(folder)
        if info.get('mimeType') != FOLDER_MIME or len(info.get('parents', [])) != 1:
            return False
        folder = info['parents'][0]
    return False


def save_registry(accounts, revision, actor):
    if isinstance(accounts, list):
        accounts = copy.deepcopy(accounts)
        for a in accounts:
            if isinstance(a, dict):
                value = a.get('folder_id', '')
                if isinstance(value, str):
                    match = re.fullmatch(r'https://drive\.google\.com/drive/(?:u/\d+/)?folders/([A-Za-z0-9_-]+)(?:\?[^\s]*)?', value.strip())
                    if match:
                        a['folder_id'] = match.group(1)
    validate_accounts(accounts)
    with _registry_lock:
        current = load_registry(fresh=True)
        if revision != current['revision']:
            raise ServiceError('Account settings changed. Refresh before saving.', 409)
        if {a['account'] for a in current['accounts']} - {a['account'] for a in accounts}:
            raise ServiceError('Keep historical accounts in the register; mark them closed instead of deleting them.', 400)
        prior = {a['account']: a for a in current['accounts']}
        for a in accounts:
            if not a.get('folder_id') or (prior.get(a['account'], {}).get('folder_id') != a['folder_id'] and not _managed_folder(a['folder_id'])):
                raise ServiceError('Account folders must belong to Cris’s statement Drive roots.', 400)
            extra = a.get('additional_folder_ids', [])
            if not isinstance(extra, list) or len(extra) > 10 or any(not isinstance(f, str) for f in extra):
                raise ServiceError('Additional historical folders are invalid.', 400)
            for folder in set(extra) - set(prior.get(a['account'], {}).get('additional_folder_ids', [])):
                if not _managed_folder(folder):
                    raise ServiceError('Historical folders must belong to Cris’s statement Drive roots.', 400)
        snapshot = dict(revision=uuid.uuid4().hex, parent_revision=revision, accounts=accounts,
                        actor=str(actor), recorded_at=datetime.now(timezone.utc).isoformat())
        data = json.dumps(snapshot, ensure_ascii=False).encode()
        Drive.upload_file(f"agm-accounting-registry-{snapshot['revision']}.json", 'application/json', base64.b64encode(data).decode(), BANK_ROOT)
        clear_cache()
        # Detect cross-worker conflicts after the immutable append, preserving both edits.
        return load_registry(fresh=True)


def _walk(folder, year):
    def load():
        queue = [(folder, '')]
        files = []
        visited = set()
        while queue:
            current, path = queue.pop(0)
            if current in visited:
                raise StatementError('A source folder is referenced more than once.')
            visited.add(current)
            if len(visited) > 150 or len(files) > 3000:
                raise StatementError('The statement inventory exceeds its safe limit.')
            for f in Drive.get_files_in_folder(current):
                if f.get('mimeType') == FOLDER_MIME:
                    named_year = re.search(r'(?<!\d)(20\d{2})(?!\d)', f['name'])
                    if not named_year or int(named_year.group(1)) == year:
                        queue.append((f['id'], path + '/' + f['name']))
                elif not f['name'].startswith('agm-accounting-'):
                    files.append(dict(f, path=path, key=f['id']))
        return files
    return _cached(('inventory', folder, year), load)


def source_bytes(source):
    if int(source.get('size', 0)) > MAX_FILE:
        raise StatementError('An original file exceeds the 25 MB limit.')
    if source.get('mimeType', '').startswith('application/vnd.google-apps.'):
        raise StatementError('This source is a Google document, not a preserved original file.')
    def load():
        data = Drive.download_file(source['id'], parse=False)
        if len(data) > MAX_FILE:
            raise StatementError('An original file exceeds the 25 MB limit.')
        return data
    data = _cached(('bytes', source['id'], source.get('modifiedTime')), load, ttl=120)
    if source.get('archive_member'):
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            info = archive.getinfo(source['archive_member'])
            if info.file_size > MAX_FILE or info.file_size > max(1, info.compress_size) * 200:
                raise StatementError('Archived original exceeds the extraction limit.')
            data = archive.read(info)
    return data


def _dates_in_name(name):
    uploaded = re.match(r'^[A-Za-z0-9-]+_(20\d{2})([01]\d)__', name)
    if uploaded:
        return [(int(uploaded.group(1)), int(uploaded.group(2)))], True
    days = []
    for match in re.finditer(r'(?<!\d)(20\d{2})[-_]?([01]\d)[-_]?([0-3]\d)?(?!\d)', name):
        days.append((int(match.group(1)), int(match.group(2))))
    for match in re.finditer(r'(?<!\d)([01]\d)[-_]([0-3]\d)[-_](20\d{2})(?!\d)', name):
        days.append((int(match.group(3)), int(match.group(1))))
    # Filename formats such as 8706_202307-0930 are quarterly, not monthly.
    quarterly = bool(re.search(r'20\d{2}[01]\d-\d{4}', name))
    quarter_end = re.search(r'(20\d{2})([01]\d)-(\d{2})\d{2}', name)
    if quarter_end:
        days.append((int(quarter_end.group(1)), int(quarter_end.group(3))))
    return sorted(set(days)), not quarterly and len(set(days)) == 1 and 'activity_' not in name.lower()


def _belongs(source, account):
    identity = account['source_account'] if account['institution'] == 'IBKR' else account['suffix']
    context = source['name'] + '/' + source.get('path', '')
    return bool(re.search(r'(?<![A-Za-z0-9])' + re.escape(identity) + r'(?![A-Za-z0-9])', context)) or bool(re.search(re.escape(identity) + r'_', context))


def account_sources(account, year):
    # Month detail/continuity reads share the expensive archive and raw-file
    # classification, keyed by the entire account policy and source selection.
    key = ('account_sources', json.dumps(account, sort_keys=True), year)
    return _cached(key, lambda: _account_sources(account, year))


def _account_sources(account, year):
    files = []
    for folder in [account['folder_id'], *account.get('additional_folder_ids', [])]:
        files.extend(_walk(folder, year))
    # Account-named IBKR folders accept only this account's filenames.
    assigned = [f for f in files if _belongs(f, account)]
    warnings = []
    if account['institution'] == 'IBKR':
        foreign = [f['name'] for f in files if re.match(r'[IUF]\d+_20\d{2}', f['name']) and not _belongs(f, account)]
        if foreign:
            warnings.append('The configured IBKR folder contains statements for a different account.')
    expanded = []
    for f in assigned:
        extension = f['name'].rsplit('.', 1)[-1].lower()
        if extension == 'zip':
            archived_year = re.search(r'(?<!\d)(20\d{2})(?!\d)', f['name'])
            if archived_year and int(archived_year.group(1)) != year:
                continue
            with zipfile.ZipFile(io.BytesIO(source_bytes(f))) as archive:
                if len(archive.infolist()) > 1000:
                    raise StatementError('Too many files in an original archive.')
                for member in archive.infolist():
                    if not member.is_dir() and member.filename.lower().endswith(('.pdf', '.csv', '.xls', '.xlsx')):
                        expanded.append(dict(f, name=Path(member.filename).name, archive_member=member.filename,
                                             key=f['id'] + '#' + member.filename))
        elif extension in {'pdf', 'csv', 'xls', 'xlsx'}:
            expanded.append(f)
    periods = {m: {'pdf': [], 'csv': [], 'alternative_raw': []} for m in range(1, 13)}
    unresolved = []
    for f in expanded:
        extension = f['name'].rsplit('.', 1)[-1].lower()
        # Some bank CSV originals were uploaded with a .pdf filename. Verify
        # their contents before classifying; preserve the source name/bytes.
        if extension == 'pdf' and f.get('mimeType') == 'text/csv' and not f.get('archive_member') and account['institution'] != 'IBKR':
            content = source_bytes(f)
            if not content.startswith(b'%PDF-'):
                try:
                    parse_bank_transactions(content, account, 'csv')
                except StatementError:
                    pass
                else:
                    extension = 'csv'
        dates, monthly = _dates_in_name(f['name'])
        if extension == 'pdf' and not monthly:
            unresolved.append(f['name'])
            continue
        if dates and (monthly and not any(y == year for y, _ in dates) or year > dates[-1][0]):
            continue
        months = [m for y, m in dates if y == year and 1 <= m <= 12]
        if len(dates) > 1:
            first, last = dates[0], dates[-1]
            months = [m for m in range(1, 13) if first <= (year, m) <= last]
        if (not dates or not monthly) and extension != 'pdf' and account['institution'] != 'IBKR':
            try:
                tx = parse_bank_transactions(source_bytes(f), account, extension)
                months = sorted({int(t['posted_date'][5:7]) for t in tx if int(t['posted_date'][:4]) == year})
            except StatementError:
                unresolved.append(f['name'])
                continue
        if not months:
            continue
        f = dict(f, source_format=extension, monthly=monthly, original=not f.get('mimeType', '').startswith('application/vnd.google-apps.'))
        for m in months:
            kind = 'pdf' if extension == 'pdf' else 'csv' if extension == 'csv' else 'alternative_raw'
            periods[m][kind].append(f)
    return periods, warnings, unresolved


def _pick(candidates, selection):
    if selection:
        return next((f for f in candidates if f['key'] == selection), None)
    return candidates[0] if len(candidates) == 1 else None


def _expected(account, year, month, today):
    start = date(year, month, 1)
    end = date(year, month, calendar.monthrange(year, month)[1])
    if end >= today.replace(day=1):
        return False, 'Current or future month is not yet due.'
    if account.get('opened_on') and end < date.fromisoformat(account['opened_on']):
        return False, 'Before account opening.'
    if account.get('closed_on') and start > date.fromisoformat(account['closed_on']):
        return False, 'After account closing.'
    if not account.get('opened_on') or account['status'] == 'closed' and not account.get('closed_on') and not account.get('ignore_unknown_closing_date'):
        return None, 'Opening/closing date needs confirmation.'
    if account['status'] == 'closed' and not account.get('closed_on') and account.get('ignore_unknown_closing_date'):
        return True, 'Closing date is unavailable; its cutoff is omitted by request.'
    return True, None


def _month_row(account, year, month, sources, warnings, today):
    expected, reason = _expected(account, year, month, today)
    choices = account.get('source_selections', {}).get(f'{year}-{month:02}', {})
    pdf = _pick(sources['pdf'], choices.get('pdf'))
    csv = _pick(sources['csv'], choices.get('raw'))
    raw = csv or _pick(sources['alternative_raw'], choices.get('raw'))
    errors = list(warnings)
    if account.get('identity_review_required'):
        errors.append(account.get('identity_note', 'Account identity requires confirmation.'))
    if choices.get('pdf') and not pdf or choices.get('raw') and not raw:
        errors.append('The selected source no longer exists in this account/month.')
    if len(sources['pdf']) > 1 and not pdf or len(sources['csv']) > 1 and not csv:
        errors.append('Duplicate originals require an explicit source selection.')
    backed_up = bool(pdf and pdf.get('original') and csv and csv.get('original') and csv.get('monthly') and not errors)
    status = 'not_due' if expected is False else 'configuration_required' if expected is None else 'source_exception' if errors else 'missing_files' if not backed_up else 'pending'
    return dict(account=account['account'], institution=account['institution'], year=year, month=month,
                currency=account['currency'], eom=f'{year}-{month:02}-{calendar.monthrange(year, month)[1]:02}',
                required=expected, requirement_note=reason, status=status, backed_up=backed_up,
                sources=sources, selected_pdf=pdf, selected_raw=raw, errors=errors, reconciled=False,
                continuity={'status': 'pending'}, analysis=None)


def inventory_report(year, account_id=None, *, today=None):
    year, _ = period(year)
    registry = load_registry()
    accounts = registry['accounts']
    if account_id:
        accounts = [a for a in accounts if a['account'] == account_id]
        if not accounts:
            raise ServiceError('Accounting account not found.', 404)
    today = today or datetime.now(ZoneInfo('America/Costa_Rica')).date()
    rows = []
    unresolved = []
    for a in accounts:
        try:
            periods, warnings, unknown = account_sources(a, year)
            unresolved.extend(dict(account=a['account'], name=name) for name in unknown)
        except (StatementError, ServiceError):
            periods = {m: {'pdf': [], 'csv': [], 'alternative_raw': []} for m in range(1, 13)}
            warnings = ['Statement folder could not be read. The account was not excluded from coverage.']
        rows.extend(_month_row(a, year, m, periods[m], warnings, today) for m in range(1, 13))
    return dict(year=year, accounts=accounts, revision=registry['revision'], rows=rows,
                unresolved_sources=unresolved, kpis=kpis(rows), as_of=today.isoformat())


def kpis(rows):
    required = [r for r in rows if r['required'] is True]
    unknown = [r for r in rows if r['required'] is None]
    backed = sum(r['backed_up'] for r in required)
    reconciled = sum(r['reconciled'] for r in required)
    return dict(required_months=len(required), required_files=len(required) * 2, backed_up_months=backed,
                missing_or_invalid_months=len(required) - backed, reconciled_months=reconciled,
                audit_ready_months=sum(r['backed_up'] and r['reconciled'] for r in required),
                unresolved_months=len(unknown), pending_analysis=sum(r['status'] == 'pending' for r in required),
                all_statements_backed_up=bool(required) and not unknown and backed == len(required),
                all_data_reconciled=bool(required) and not unknown and reconciled == len(required))


def pdf_text(content, *, ocr=False):
    if not content.startswith(b'%PDF-'):
        raise StatementError('The original PDF is invalid.')
    try:
        command = [sys.executable, str(Path(__file__).parents[3] / 'utils/accounting_pdf.py')]
        if ocr:
            command.append('--ocr')
        result = subprocess.run(command, input=content, capture_output=True, check=True, timeout=90)
        return json.loads(result.stdout)['text']
    except (subprocess.SubprocessError, OSError, ValueError, KeyError):
        raise StatementError('PDF text could not be extracted. Check the original and local OCR availability.') from None


def _summary(account, year, month, row):
    if account['institution'] == 'IBKR':
        if not row['selected_raw'] or not row['selected_raw']['name'].lower().endswith('.csv'):
            raise StatementError('An original IBKR CSV is required.')
        return parse_ibkr(source_bytes(row['selected_raw']), account, year, month)
    if not row['selected_pdf']:
        raise StatementError('Select an unambiguous original PDF for the statement totals.')
    source = row['selected_pdf']
    def load():
        content = source_bytes(source)
        text = pdf_text(content)
        mode = 'native'
        if len(text.strip()) < 80:
            text = pdf_text(content, ocr=True)
            mode = 'local_ocr'
        result = parse_bank_summary(text, account, year, month)
        result['extraction'] = mode
        return result
    return _cached(('summary', source['key'], source.get('modifiedTime'), account['source_account'], account['currency'], year, month), load, ttl=120)


def analyze_month(account_id, year, month):
    if not isinstance(account_id, str) or not account_id:
        raise ServiceError('Select an accounting account.', 400)
    year, month = period(year, month)
    with _analysis_slots:
        report = inventory_report(year, account_id)
        a = report['accounts'][0]
        row = report['rows'][month - 1]
        try:
            summary = _summary(a, year, month, row)
            if summary.get('coverage_error'):
                row['errors'].append(summary['coverage_error'])
                row['backed_up'] = False
            raw = row['selected_raw']
            if not raw:
                raise StatementError('An unambiguous transaction original is required for reconciliation.')
            tx = summary.pop('transactions') if a['institution'] == 'IBKR' else parse_bank_transactions(source_bytes(raw), a, raw.get('source_format') or raw['name'].rsplit('.', 1)[-1].lower())
            tx = [t for t in tx if t['posted_date'][:7] == f'{year}-{month:02}']
            for item in tx:
                item.update(account=account_id, eom=row['eom'], currency=summary['currency'])
            result = reconcile(summary, tx)
            continuity = {'status': 'not_applicable'}
            first_month = (a.get('opened_on') or '')[:7] == f'{year}-{month:02}'
            if not first_month:
                previous_year, previous_month = (year, month - 1) if month > 1 else (year - 1, 12)
                previous = inventory_report(previous_year, account_id)['rows'][previous_month - 1]
                try:
                    prior = _summary(a, previous_year, previous_month, previous)
                    difference = summary['opening'] - prior['ending']
                    continuity = dict(status='passed' if abs(difference) < Decimal('0.01') else 'mismatch',
                                      difference=difference, previous_period=f'{previous_year}-{previous_month:02}')
                except (StatementError, ServiceError):
                    continuity = dict(status='unavailable', previous_period=f'{previous_year}-{previous_month:02}')
            row['analysis'] = dict(summary=summary, checks=result, transaction_count=len(tx),
                                   sources=[dict(key=f['key'], id=f['id'], name=f['name'], modified_time=f.get('modifiedTime'),
                                                 sha256=hashlib.sha256(source_bytes(f)).hexdigest()) for f in [row['selected_pdf'], raw] if f])
            row['transactions'] = tx
            row['continuity'] = continuity
            row['reconciled'] = bool(result['reconciled'] and continuity['status'] in {'passed', 'not_applicable'} and not row['errors'] and row['required'] is not None)
            row['status'] = 'configuration_required' if row['required'] is None else 'source_exception' if row['errors'] else 'reconciled' if row['reconciled'] and row['backed_up'] else 'review'
        except StatementError as exc:
            row['errors'].append(str(exc))
            row['status'] = 'parse_error'
            row['backed_up'] = False
        return _json_numbers(row)


def download_original(account_id, year, month, key):
    if not isinstance(account_id, str) or not account_id:
        raise ServiceError('Select an accounting account.', 400)
    report = inventory_report(year, account_id)
    _, month = period(year, month)
    sources = report['rows'][month - 1]['sources']
    source = next((f for group in sources.values() for f in group if f['key'] == key), None)
    if not source:
        raise ServiceError('Original file is not part of this account/month inventory.', 404)
    content = source_bytes(source)
    return content, source['name']


def annual_bundle(year, account_id=None, institution=None):
    report = inventory_report(year, account_id)
    if institution:
        report['accounts'] = [a for a in report['accounts'] if a['institution'] == institution]
        report['rows'] = [r for r in report['rows'] if r['institution'] == institution]
        if not report['accounts']:
            raise ServiceError('Accounting institution not found.', 404)
        report['kpis'] = kpis(report['rows'])
    output = io.BytesIO()
    manifest = dict(year=report['year'], generated_at=datetime.now(timezone.utc).isoformat(), registry_revision=report['revision'],
                    accounts=report['accounts'], inventory=report['rows'], coverage=report['kpis'], files=[],
                    download_errors=[],
                    notice='Original bytes are preserved. This inventory is not a certification of historical immutability or reconciliation. Missing evidence and unresolved account dates are listed.')
    seen = set()
    total = 0
    with zipfile.ZipFile(output, 'w', zipfile.ZIP_DEFLATED) as archive:
        for row in report['rows']:
            for group in row['sources'].values():
                for source in group:
                    if source['key'] in seen:
                        continue
                    seen.add(source['key'])
                    if len(seen) > 500:
                        raise ServiceError('Select one account to keep the annual bundle within its limit.', 413)
                    try:
                        data = source_bytes(source)
                    except (StatementError, ServiceError):
                        manifest['download_errors'].append(dict(account=row['account'], source_key=source['key'], name=source['name'], reason='Original could not be downloaded; it is not included in this bundle.'))
                        continue
                    total += len(data)
                    if total > MAX_BUNDLE:
                        raise ServiceError('Select one account to keep the annual bundle below 128 MB.', 413)
                    name = f"{row['account']}/{hashlib.sha256(source['key'].encode()).hexdigest()[:10]}_{Path(source['name']).name}"
                    archive.writestr(name, data)
                    manifest['files'].append(dict(path=name, drive_file_id=source['id'], source_name=source['name'],
                                                  archive_member=source.get('archive_member'), modified_time=source.get('modifiedTime'),
                                                  size=len(data), sha256=hashlib.sha256(data).hexdigest()))
        manifest['backup_complete'] = bool(report['kpis']['all_statements_backed_up'] and not manifest['download_errors'])
        archive.writestr('manifest.json', json.dumps(manifest, ensure_ascii=False, indent=2))
    scope = account_id or (re.sub(r'[^A-Za-z0-9-]', '-', institution) if institution else 'all-accounts')
    suffix = '' if manifest['backup_complete'] else '-incomplete'
    return output.getvalue(), f"accounting-{year}-{scope}{suffix}.zip"


def upload_original(account_id, year, month, original_name, encoded, actor):
    year, month = period(year, month)
    registry = load_registry()
    a = next((a for a in registry['accounts'] if a['account'] == account_id), None)
    if not a:
        raise ServiceError('Accounting account not found.', 404)
    if not isinstance(original_name, str) or not original_name or len(original_name) > 250:
        raise ServiceError('An original filename is required.', 400)
    ext = Path(original_name).suffix.lower().lstrip('.')
    if ext not in {'pdf', 'csv', 'xls', 'xlsx'} or not isinstance(encoded, str) or len(encoded) > MAX_FILE * 4 // 3 + 4:
        raise ServiceError('Upload an original PDF, CSV, XLS or XLSX no larger than 25 MB.', 400)
    try:
        content = base64.b64decode(encoded, validate=True)
    except (ValueError, binascii.Error):
        raise ServiceError('The uploaded file encoding is invalid.', 400) from None
    if not content or len(content) > MAX_FILE:
        raise ServiceError('The original is empty or exceeds 25 MB.', 400)
    try:
        if ext == 'pdf':
            if a['institution'] == 'IBKR':
                text = pdf_text(content)
                month_name = datetime(year, month, 1).strftime('%B')
                period_patterns = [rf'{month_name}\s+\d{{1,2}},?\s+{year}', rf'{year}-{month:02}', rf'{month:02}/\d{{2}}/{year}']
                if not re.search(re.escape(a['source_account']), text) or not any(re.search(p, text[:3000], re.I) for p in period_patterns):
                    raise StatementError('IBKR PDF identity or period could not be verified.')
            else:
                text = pdf_text(content)
                parse_bank_summary(text if len(text.strip()) >= 80 else pdf_text(content, ocr=True), a, year, month)
        elif a['institution'] == 'IBKR':
            parse_ibkr(content, a, year, month)
        else:
            tx = parse_bank_transactions(content, a, ext)
            if any(t['posted_date'][:7] != f'{year}-{month:02}' for t in tx):
                raise StatementError('Upload a monthly export; transactions extend beyond the selected month.')
    except StatementError as exc:
        raise ServiceError(str(exc), 400) from None
    periods, _, _ = account_sources(a, year)
    kind = 'pdf' if ext == 'pdf' else 'csv' if ext == 'csv' else 'alternative_raw'
    if periods[month][kind]:
        raise ServiceError('An original for this account/month/format already exists. Review its source instead of replacing it.', 409)
    name = f"{a['account']}_{year}{month:02}__{re.sub(r'[^A-Za-z0-9._-]', '_', Path(original_name).name)[:120]}"
    created = Drive.upload_file(name, {'pdf': 'application/pdf', 'csv': 'text/csv', 'xls': 'application/vnd.ms-excel', 'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'}[ext], base64.b64encode(content).decode(), a['folder_id'])
    evidence = dict(account=a['account'], year=year, month=month, original_name=original_name, file_id=created['id'],
                    sha256=hashlib.sha256(content).hexdigest(), size=len(content), actor=str(actor), recorded_at=datetime.now(timezone.utc).isoformat())
    clear_cache()
    try:
        Drive.upload_file(f'agm-accounting-upload-{uuid.uuid4().hex}.json', 'application/json', base64.b64encode(json.dumps(evidence).encode()).decode(), BANK_ROOT)
    except Exception:
        raise ServiceError('The original was uploaded, but its integrity receipt could not be saved. Do not upload it again; review the existing source.', 502, details={'file_id': created['id']}) from None
    return evidence
