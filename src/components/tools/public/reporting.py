from datetime import datetime, date, timedelta
from src.utils.logger import logger
import csv
import calendar
import io
import threading
import pandas as pd
import re
import json
import hashlib
import os
import pytz
from collections import Counter

from src.utils.connectors.drive import GoogleDrive
from src.utils.exception import ServiceError, handle_exception
from src.utils.connectors.ibkr_web_api import IBKRWebAPI
from src.utils.report_cache import ReportCache
from src.utils.report_reads import read_report_files
from src.utils.position_quotes import enrich_position_quotes
from src.utils.lazy_client import LazyClient
from src.utils.ibkr_compliance import compliance_report_view

logger.announcement('Initializing Reporting Service', type='info')
Drive = LazyClient(GoogleDrive)
ibkr_web_api = LazyClient(IBKRWebAPI)
_ending_balances_cache = {'key': None, 'rows': None}
_activity_statement_file_cache = {}
_ending_balances_cache_lock = threading.Lock()
_ibkr_statement_rows_cache = {}
_ibkr_statement_rows_cache_lock = threading.Lock()
_daily_report_cache = ReportCache(os.getenv('REPORT_CACHE_TTL_SECONDS', '30'), keys=(
    'clients', 'nav', 'client-fees', 'bonds', 'stocks', 'etfs', 'ust-bonds', 'open-positions', 'ibkr-details', 'compliance-tasks', 'pending-tasks', 'registration-tasks',
))
_quote_snapshot_cache = ReportCache(os.getenv('REPORT_CACHE_TTL_SECONDS', '30'), keys=('market-quotes',))

activity_statement_folders = {
    'I6413690': '1qJhG-9F_YteWY-hCP1EaIhbJQ1DW71_h',
    'F10740574': '1hhcJWVJFJwo6cKWcTBgMMExL-kdKDN9s',
    'U6413691': '1v-WwlyMJ-U59F0eBL58CDJPNrorxRw1p',
    'F2074691': '1Vt9TqUlNxn2l43AW7mM296EcpghoBlbj',
    'I285407': '1CHFgaLx5sa4rja9QX25vwglMKMRL20b9',
}

batch_folder_id = '1N3LwrG7IossvCrrrFufWMb26VOcRxhi8'
resources_folder_id = '18Gtm0jl1HRfb1B_3iGidp9uPvM5ZYhOF'
ofac_backup_folder_id = '13W9sXMbFvWtXPsEy6FiZJrQDHV3WYDD6'
uk_backup_folder_id = '1-57AG_nFE2elzOygdc7PGqdB4Y9k_7h6'
un_backup_folder_id = '1AwTRSLSi0D3kzyhFx9Be53ugvo7uJgpd'

UN_SANCTIONS_XML_URL = 'https://scsanctions.un.org/resources/xml/en/name/consolidated.xml?_gl=1*b6x5x9*_ga*MTM1Mzg4ODEzNS4xNzc3NjU0MjY4*_ga_TK9BQL5X7Z*czE3Nzc2NTQyNjgkbzEkZzEkdDE3Nzc2NTU1NzEkajYwJGwwJGgw'

logger.announcement('Initialized Reporting Service', type='success')

"""
TODAY
"""

@handle_exception
def get_clients_report():
    """
    Get the clients list.
    
    :return: Response object with clients list or error message
    """
    return _cached_daily_report('clients', 'ibkr_clients')


def _describe_daily_report(name):
    files = Drive.get_files_in_folder(resources_folder_id, include_report_metadata=True)
    matches = [file for file in files if name in file['name']]
    if len(matches) != 1:
        raise Exception(f'{name} file not found or multiple files found')
    source = matches[0]
    downloadable = (source.get('capabilities') or {}).get('canDownload')
    if downloadable is False:
        raise ServiceError('Reporting source download is unavailable.', 503, code='report_source_unavailable')
    version = (source['id'], source['version'], source['modifiedTime'], source.get('size'), source.get('mimeType')) if source.get('version') and source.get('modifiedTime') and downloadable is True else None
    return version, source


def _read_daily_report(name):
    _, source = _describe_daily_report(name)
    return Drive.download_file(file_id=source['id'], parse=True)


def _cached_daily_report(key, name):
    return _daily_report_cache.get_versioned(key, lambda: _describe_daily_report(name),
                                           lambda source: Drive.download_file(file_id=source['id'], parse=True))


@handle_exception
def get_compliance_tasks_report(account_id=None):
    if account_id is not None and not re.fullmatch(r'[A-Z]\d+', account_id):
        raise ServiceError('Provide a valid IBKR account_id.', 400)

    def read():
        matches = [file for file in Drive.get_files_in_folder(resources_folder_id)
                   if file.get('name') == 'ibkr_compliance_tasks.json']
        if len(matches) != 1:
            raise ServiceError('Compliance report missing or duplicated. Supply compliance_tasks.csv to Clients ETL.',
                               503, code='compliance_report_unavailable')
        return Drive.download_file(file_id=matches[0]['id'], parse=True)

    # Filter after copying the shared base; never cache one account's projection.
    return compliance_report_view(_daily_report_cache.get('compliance-tasks', read), account_id)


@handle_exception
def get_task_snapshot_rows(source):
    from src.components.tools.private.ibkr_task_refresh import decode_snapshot
    if source not in ('pending', 'registration'):
        raise ServiceError('Unsupported task report source.', 400)
    filename = f'ibkr_{source}_tasks.csv'

    def describe():
        matches = [file for file in Drive.get_files_in_folder(resources_folder_id, include_report_metadata=True)
                   if file.get('name') == filename]
        if len(matches) != 1:
            raise ServiceError('Task snapshot missing or duplicated. Run the Clients ETL task refresh.',
                               503, code='task_report_unavailable')
        file = matches[0]
        downloadable = (file.get('capabilities') or {}).get('canDownload')
        if downloadable is False:
            raise ServiceError('Task snapshot download unavailable.', 503, code='task_report_unavailable')
        version = (file['id'], file['version'], file['modifiedTime']) if file.get('version') and file.get('modifiedTime') and downloadable is True else None
        return version, file

    return _daily_report_cache.get_versioned(f'{source}-tasks', describe,
        lambda file: decode_snapshot(Drive.download_file(file_id=file['id'], parse=False), source))


def get_tasks_review(args):
    from src.utils.task_review import review_view
    feeds, health = {}, {}
    for source in ('compliance', 'pending', 'registration'):
        try:
            data = get_compliance_tasks_report() if source == 'compliance' else get_task_snapshot_rows(source)
            feeds[source] = data
            if source == 'compliance':
                health[source] = {'status': 'stale' if data['freshness']['stale'] else 'fresh',
                    'records': len(data['rows']), 'updated_at': data['source']['uploaded_at'],
                    'exported_at': None}
            else:
                coverage = [row for row in data if row['record_type'] == 'account']
                from src.utils.task_review import freshness
                counts = Counter(freshness(row['refresh_status'], row['last_success_at']) for row in coverage)
                health[source] = {'status': 'partial' if counts['stale'] or counts['unavailable'] else 'fresh',
                    'records': sum(row['record_type'] == 'task' for row in data),
                    'accounts': len(coverage), 'fresh': counts['fresh'], 'stale': counts['stale'],
                    'unavailable': counts['unavailable'],
                    'updated_at': max((row['last_attempt_at'] for row in coverage), default=None)}
        except Exception:
            health[source] = {'status': 'unavailable', 'records': None, 'updated_at': None}
    if not feeds:
        raise ServiceError('All task snapshots are unavailable. Check the Clients ETL publication.', 503,
                           code='task_report_unavailable')
    try:
        return review_view(feeds, health, args)
    except (TypeError, ValueError):
        raise ServiceError('Invalid task review filters.', 400)

@handle_exception
def get_client_fees_report():
    """Read a daily snapshot under the bounded report freshness policy."""
    return _cached_daily_report('client-fees', 'ibkr_client_fees')

@handle_exception
def get_nav_report():
    """
    Get the NAV report.
    
    :return: Response object with NAV report or error message
    """
    return _cached_daily_report('nav', 'ibkr_nav')

@handle_exception
def get_bond_report():
    """Read a daily snapshot under the bounded report freshness policy."""
    return _cached_daily_report('bonds', 'ibkr_bonds_snapshot')

@handle_exception
def get_stocks_report():
    """Read a daily snapshot under the bounded report freshness policy."""
    return _cached_daily_report('stocks', 'ibkr_stocks_snapshot')

@handle_exception
def get_etfs_report():
    """Read a daily snapshot under the bounded report freshness policy."""
    return _cached_daily_report('etfs', 'ibkr_etfs_snapshot')

@handle_exception
def get_ust_bond_report():
    """Read a daily snapshot under the bounded report freshness policy."""
    return _cached_daily_report('ust-bonds', 'ibkr_ust_bonds_snapshot')

def _read_quote_snapshots():
    files = Drive.get_files_in_folder(resources_folder_id)
    snapshots = []
    for name, asset_class in [('ibkr_bonds_snapshot.csv', 'BOND'), ('ibkr_ust_bonds_snapshot.csv', 'BOND'), ('ibkr_stocks_snapshot.csv', 'STK'), ('ibkr_etfs_snapshot.csv', 'STK')]:
        matches = [item for item in files if item['name'] == name]
        if len(matches) != 1:
            raise ServiceError(f'{name} missing or duplicated; quote report unavailable', 503)
        item = matches[0]
        snapshots.append({'name': name, 'asset_class': asset_class, 'modified_time': item.get('modifiedTime'),
                          'rows': Drive.download_file(file_id=item['id'], parse=True)})
    return snapshots


@handle_exception
def add_open_position_quotes(positions):
    if not positions:
        return []
    snapshots = _quote_snapshot_cache.get('market-quotes', _read_quote_snapshots)
    return enrich_position_quotes(positions, snapshots)


@handle_exception
def get_open_positions_report(include_quotes=False, account_ids=None):
    """
    Get the open positions report.
    
    :return: Response object with open positions report or error message
    """
    open_positions = _cached_daily_report('open-positions', 'ibkr_open_positions_all')
    if account_ids is not None:
        selected = {str(value).strip() for value in account_ids}
        open_positions = [row for row in open_positions if str(row.get('ClientAccountID') or '').strip() in selected]
    return add_open_position_quotes(open_positions) if include_quotes else open_positions

@handle_exception
def get_ofac_sdn_list():
    files_in_resources_folder = Drive.get_files_in_folder(resources_folder_id)
    ofac_sdn_list_file = [ofac_sdn_list for ofac_sdn_list in files_in_resources_folder if 'ofac_sdn_list' in ofac_sdn_list['name']]
    if len(ofac_sdn_list_file) != 1:
        logger.error('OFAC SDN list file not found or multiple files found')
        raise Exception('OFAC SDN list file not found or multiple files found')
    ofac_sdn_list = Drive.download_file(file_id=ofac_sdn_list_file[0]['id'], parse=True)
    return ofac_sdn_list

def _normalize_day_reference(day_reference) -> date:
    if isinstance(day_reference, datetime):
        return day_reference.date()
    if isinstance(day_reference, date):
        return day_reference
    if isinstance(day_reference, str):
        return datetime.strptime(day_reference, '%Y-%m-%d').date()
    raise ValueError('Invalid day_reference. Use date, datetime, or YYYY-MM-DD string.')

def _rows_signature(rows: list) -> str:
    canonical_rows = []
    for row in rows or []:
        if isinstance(row, dict):
            canonical_rows.append(dict(sorted(row.items())))
        else:
            canonical_rows.append(row)
    canonical_rows.sort(key=lambda item: json.dumps(item, sort_keys=True, default=str))
    payload = json.dumps(canonical_rows, sort_keys=True, default=str).encode('utf-8')
    return hashlib.sha256(payload).hexdigest()

def _canonicalize_row(row) -> str:
    if isinstance(row, dict):
        normalized = dict(sorted(row.items()))
    else:
        normalized = row
    return json.dumps(normalized, sort_keys=True, default=str)

def _row_change_summary(today_rows: list, yesterday_rows: list, sample_size: int = 3) -> dict:
    today_map = {_canonicalize_row(row): row for row in (today_rows or [])}
    yesterday_map = {_canonicalize_row(row): row for row in (yesterday_rows or [])}
    today_counts = Counter(_canonicalize_row(row) for row in (today_rows or []))
    yesterday_counts = Counter(_canonicalize_row(row) for row in (yesterday_rows or []))

    added_keys = sorted(
        key for key, count in (today_counts - yesterday_counts).items()
        if count > 0
    )
    removed_keys = sorted(
        key for key, count in (yesterday_counts - today_counts).items()
        if count > 0
    )

    return {
        'added_count': sum((today_counts - yesterday_counts).values()),
        'removed_count': sum((yesterday_counts - today_counts).values()),
        'added_samples': [today_map[k] for k in added_keys[:sample_size]],
        'removed_samples': [yesterday_map[k] for k in removed_keys[:sample_size]],
    }

@handle_exception
def get_ofac_sdn_backup_for_day(day_reference) -> dict | None:
    return _get_sanctions_backup_for_day(
        day_reference=day_reference,
        backup_folder_id=ofac_backup_folder_id,
        file_prefix='ofac_sdn_list_',
    )

@handle_exception
def get_uk_sanctions_backup_for_day(day_reference) -> dict | None:
    return _get_sanctions_backup_for_day(
        day_reference=day_reference,
        backup_folder_id=uk_backup_folder_id,
        file_prefix='uk_sanctions_list_',
    )

@handle_exception
def get_un_sanctions_backup_for_day(day_reference) -> dict | None:
    return _get_sanctions_backup_for_day(
        day_reference=day_reference,
        backup_folder_id=un_backup_folder_id,
        file_prefix='un_sanctions_list_',
    )

def _get_sanctions_backup_for_day(day_reference, backup_folder_id: str, file_prefix: str) -> dict | None:
    target_day = _normalize_day_reference(day_reference)
    day_prefix = target_day.strftime('%Y%m%d')
    target_prefix = f'{file_prefix}{day_prefix}'

    files_in_backup_folder = Drive.get_files_in_folder(backup_folder_id) or []
    matching_files = [
        f for f in files_in_backup_folder
        if f.get('name', '').startswith(target_prefix)
    ]
    if not matching_files:
        return None

    matching_files.sort(key=lambda f: f.get('name', ''), reverse=True)
    selected_file = matching_files[0]
    rows = Drive.download_file(file_id=selected_file['id'], parse=True) or []
    return {
        'day': target_day.isoformat(),
        'file_id': selected_file.get('id'),
        'file_name': selected_file.get('name'),
        'rows': rows,
        'rows_signature': _rows_signature(rows),
    }

@handle_exception
def compare_ofac_sdn_today_vs_yesterday(reference_day: date | None = None) -> dict:
    return _compare_sanctions_today_vs_yesterday(
        snapshot_fetcher=get_ofac_sdn_backup_for_day,
        reference_day=reference_day,
    )

@handle_exception
def compare_uk_sanctions_today_vs_yesterday(reference_day: date | None = None) -> dict:
    return _compare_sanctions_today_vs_yesterday(
        snapshot_fetcher=get_uk_sanctions_backup_for_day,
        reference_day=reference_day,
    )

@handle_exception
def compare_un_sanctions_today_vs_yesterday(reference_day: date | None = None) -> dict:
    return _compare_sanctions_today_vs_yesterday(
        snapshot_fetcher=get_un_sanctions_backup_for_day,
        reference_day=reference_day,
    )

def _compare_sanctions_today_vs_yesterday(snapshot_fetcher, reference_day: date | None = None) -> dict:
    today = reference_day or datetime.now(pytz.timezone('America/Costa_Rica')).date()
    yesterday = today - timedelta(days=1)

    today_snapshot = snapshot_fetcher(today)
    yesterday_snapshot = snapshot_fetcher(yesterday)
    missing_today = not bool(today_snapshot)
    missing_yesterday = not bool(yesterday_snapshot)

    change_summary = None
    if today_snapshot and yesterday_snapshot:
        if today_snapshot.get('rows_signature') != yesterday_snapshot.get('rows_signature'):
            change_summary = _row_change_summary(
                today_rows=today_snapshot.get('rows') or [],
                yesterday_rows=yesterday_snapshot.get('rows') or [],
            )

    return {
        'today': today.isoformat(),
        'yesterday': yesterday.isoformat(),
        'today_snapshot': today_snapshot,
        'yesterday_snapshot': yesterday_snapshot,
        'missing_today': missing_today,
        'missing_yesterday': missing_yesterday,
        'both_available': not missing_today and not missing_yesterday,
        'is_same': bool(
            today_snapshot
            and yesterday_snapshot
            and today_snapshot.get('rows_signature') == yesterday_snapshot.get('rows_signature')
        ),
        'change_summary': change_summary,
    }

@handle_exception
def compare_all_sanctions_today_vs_yesterday(reference_day: date | None = None) -> dict:
    ofac = compare_ofac_sdn_today_vs_yesterday(reference_day=reference_day)
    uk = compare_uk_sanctions_today_vs_yesterday(reference_day=reference_day)
    un = compare_un_sanctions_today_vs_yesterday(reference_day=reference_day)

    return {
        'ofac': ofac,
        'uk': uk,
        'un': un,
        'all_available': all(
            isinstance(c, dict) and c.get('both_available')
            for c in [ofac, uk, un]
        ),
        'all_same': all(
            isinstance(c, dict) and c.get('is_same')
            for c in [ofac, uk, un]
        )
    }

@handle_exception
def get_uk_sanctions_list():
    files_in_resources_folder = Drive.get_files_in_folder(resources_folder_id)
    uk_sanctions_list_file = [uk_sanctions_list for uk_sanctions_list in files_in_resources_folder if 'uk_sanctions_list' in uk_sanctions_list['name']]
    if len(uk_sanctions_list_file) != 1:
        logger.error('UK sanctions list file not found or multiple files found')
        raise Exception('UK sanctions list file not found or multiple files found')
    uk_sanctions_list = Drive.download_file(file_id=uk_sanctions_list_file[0]['id'], parse=True)
    return uk_sanctions_list

@handle_exception
def get_un_sanctions_list():
    files_in_resources_folder = Drive.get_files_in_folder(resources_folder_id)
    un_sanctions_list_file = [un_sanctions_list for un_sanctions_list in files_in_resources_folder if 'un_sanctions_list' in un_sanctions_list['name']]
    if len(un_sanctions_list_file) != 1:
        logger.error('UN sanctions list file not found or multiple files found')
        raise Exception('UN sanctions list file not found or multiple files found')
    un_sanctions_list = Drive.download_file(file_id=un_sanctions_list_file[0]['id'], parse=True)
    return un_sanctions_list

@handle_exception
def get_deposits_withdrawals():
    files_in_resources_folder = Drive.get_files_in_folder(resources_folder_id)
    deposits_withdrawals_file = [deposits_withdrawals for deposits_withdrawals in files_in_resources_folder if 'ibkr_deposits_withdrawals' in deposits_withdrawals['name']]
    if len(deposits_withdrawals_file) != 1:
        logger.error('Deposits and withdrawals file not found or multiple files found')
        raise Exception('Deposits and withdrawals file not found or multiple files found')
    deposits_withdrawals = Drive.download_file(file_id=deposits_withdrawals_file[0]['id'], parse=True)
    return deposits_withdrawals

def _parse_report_datetime(value):
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    normalized = text.replace('Z', '+00:00')
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        for fmt in ('%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M'):
            try:
                parsed = datetime.strptime(text, fmt)
                break
            except ValueError:
                continue
        else:
            return None

    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(pytz.UTC).replace(tzinfo=None)

    return parsed

def _get_year_months_between(start_date: datetime, end_date: datetime):
    periods = []
    cursor = datetime(start_date.year, start_date.month, 1)
    last_month = datetime(end_date.year, end_date.month, 1)

    while cursor <= last_month:
        periods.append((str(cursor.year), f'{cursor.month:02d}'))
        if cursor.month == 12:
            cursor = datetime(cursor.year + 1, 1, 1)
        else:
            cursor = datetime(cursor.year, cursor.month + 1, 1)

    return periods

@handle_exception
def get_monthly_deposits_withdrawals(years: list, months: list, start_date=None, end_date=None):
    """
    Extract deposits/withdrawals information grouped by year and month,
    keeping only the latest file available for each month.

    :param years: List of years to include
    :param months: List of months to include (as strings, e.g., '01', '02')
    :param start_date: Inclusive lower datetime bound
    :param end_date: Inclusive upper datetime bound
    :return: List of deposits/withdrawals records
    """
    deposits_withdrawals_root_folder_id = '1ZCJfH2hxvMLuP470HMa-D33_R_l-Lhtx'

    start_date = _parse_report_datetime(start_date)
    end_date = _parse_report_datetime(end_date)

    if start_date and end_date and start_date > end_date:
        raise ServiceError('start_date cannot be after end_date', 400)

    years = [str(y).strip() for y in years if str(y).strip()]
    months = [str(m).strip().zfill(2) for m in months if str(m).strip()]

    if start_date and end_date:
        year_month_pairs = _get_year_months_between(start_date, end_date)
        years = sorted({year for year, _ in year_month_pairs})
        months = sorted({month for _, month in year_month_pairs})

    # If no months are provided, include all months in each requested year.
    if not months:
        months = [str(m).zfill(2) for m in range(1, 13)]

    try:
        root_contents = Drive.get_files_in_folder(deposits_withdrawals_root_folder_id)
    except Exception as e:
        logger.error(f'Could not fetch contents of deposits/withdrawals root folder: {deposits_withdrawals_root_folder_id}: {e}')
        return []

    all_deposits_withdrawals_dfs = []
    files_in_root = [f for f in root_contents if f['mimeType'] != 'application/vnd.google-apps.folder']

    # Deposits/withdrawals files are expected as files named like 794867_YYYYMMDD_YYYYMMDD.csv.
    # Use the last date in the filename as the period end.
    if files_in_root:
        latest_files_for_period = {}

        for f in files_in_root:
            name = f['name']
            date_matches = re.findall(r'(\d{8})', name)
            if date_matches:
                end_date_str = date_matches[-1]
            else:
                end_date_str = datetime.fromisoformat(f['modifiedTime'].replace('Z', '+00:00')).strftime('%Y%m%d')

            file_year = end_date_str[:4]
            file_month = end_date_str[4:6]

            if file_year in years and file_month in months:
                key = (file_year, file_month)
                if key not in latest_files_for_period or end_date_str > latest_files_for_period[key]['end_date']:
                    latest_files_for_period[key] = {
                        'id': f['id'],
                        'end_date': end_date_str
                    }

        for (year, month), info in latest_files_for_period.items():
            try:
                deposits_withdrawals_data = Drive.download_file(file_id=info['id'], parse=True)
                if deposits_withdrawals_data:
                    df = pd.DataFrame(deposits_withdrawals_data)
                    df['Year'] = year
                    df['Month'] = month
                    df['ReportDate'] = info['end_date']
                    all_deposits_withdrawals_dfs.append(df)
            except Exception as e:
                logger.error(f'Error downloading/parsing deposits/withdrawals file for {year}-{month}: {e}')
    else:
        # Fallback for structures that keep deposits/withdrawals files under year subfolders.
        year_folders = {
            f['name']: f['id']
            for f in root_contents
            if f['mimeType'] == 'application/vnd.google-apps.folder'
        }

        for year in years:
            if year not in year_folders:
                logger.warning(f'Year folder {year} not found in deposits/withdrawals root folder.')
                continue

            year_folder_id = year_folders[year]

            try:
                year_files = Drive.get_files_in_folder(year_folder_id)
            except Exception as e:
                logger.error(f'Could not fetch contents for deposits/withdrawals year folder {year}: {e}')
                continue

            latest_files_for_months = {}

            for f in year_files:
                if f['mimeType'] == 'application/vnd.google-apps.folder':
                    continue

                name = f['name']
                date_matches = re.findall(r'(\d{8})', name)
                if date_matches:
                    end_date_str = date_matches[-1]
                else:
                    end_date_str = datetime.fromisoformat(f['modifiedTime'].replace('Z', '+00:00')).strftime('%Y%m%d')

                file_year = end_date_str[:4]
                file_month = end_date_str[4:6]

                if file_year == year and file_month in months:
                    if file_month not in latest_files_for_months or end_date_str > latest_files_for_months[file_month]['end_date']:
                        latest_files_for_months[file_month] = {
                            'id': f['id'],
                            'end_date': end_date_str
                        }

            for month, info in latest_files_for_months.items():
                try:
                    deposits_withdrawals_data = Drive.download_file(file_id=info['id'], parse=True)
                    if deposits_withdrawals_data:
                        df = pd.DataFrame(deposits_withdrawals_data)
                        df['Year'] = year
                        df['Month'] = month
                        df['ReportDate'] = info['end_date']
                        all_deposits_withdrawals_dfs.append(df)
                except Exception as e:
                    logger.error(f'Error downloading/parsing deposits/withdrawals file for {year}-{month}: {e}')

    if not all_deposits_withdrawals_dfs:
        return []

    combined_deposits_withdrawals_df = pd.concat(all_deposits_withdrawals_dfs, ignore_index=True)
    combined_deposits_withdrawals_df = combined_deposits_withdrawals_df.fillna('')

    if start_date or end_date:
        parsed_datetimes = pd.to_datetime(combined_deposits_withdrawals_df['Date/Time'], errors='coerce')
        mask = parsed_datetimes.notna()

        if start_date:
            mask &= parsed_datetimes >= pd.Timestamp(start_date)
        if end_date:
            mask &= parsed_datetimes <= pd.Timestamp(end_date)

        combined_deposits_withdrawals_df = combined_deposits_withdrawals_df.loc[mask]

    return combined_deposits_withdrawals_df.to_dict(orient='records')

@handle_exception
def get_ibkr_details():
    """Read the daily Drive snapshot; live account/provider routes stay uncached."""
    return _cached_daily_report('ibkr-details', 'ibkr_account_details')

"""
BACKUPS
"""

@handle_exception
def get_trades_report(years: list, months: list):
    """
    Extract trades information grouped by year and month.
    
    :param years: List of years to include
    :param months: List of months to include (as strings, e.g., '01', '02')
    :return: List of trade records
    """
    trades_root_folder_id = '1Sx3zEykoK--cBolbQBEtSudGCrvJPLaY'

    # Normalize years and months to zero-padded strings for consistent comparison
    years = [str(y).strip() for y in years]
    months = [str(m).strip().zfill(2) for m in months]

    print(f'[get_trades_report] years={years}, months={months}')
    
    # 1. Get all year folders in the root trades folder
    try:
        root_contents = Drive.get_files_in_folder(trades_root_folder_id)
    except Exception as e:
        logger.error(f'Could not fetch contents of trades root folder: {trades_root_folder_id}: {e}')
        print(f'[get_trades_report] ERROR fetching root folder {trades_root_folder_id}: {e}')
        raise

    print(f'[get_trades_report] root_contents ({len(root_contents)} items):')
    for item in root_contents:
        print(f'  name={item["name"]!r}  mimeType={item["mimeType"]}  id={item["id"]}')

    year_folders = {
        f['name']: f['id'] 
        for f in root_contents 
        if f['mimeType'] == 'application/vnd.google-apps.folder'
    }

    print(f'[get_trades_report] year_folders found: {list(year_folders.keys())}')

    all_trades_dfs = []

    for year in years:
        if year not in year_folders:
            logger.warning(f'Year folder {year} not found in root folder.')
            print(f'[get_trades_report] WARNING: year folder {year!r} not in {list(year_folders.keys())}')
            continue
            
        year_folder_id = year_folders[year]
        print(f'[get_trades_report] Processing year={year}, folder_id={year_folder_id}')

        try:
            year_files = Drive.get_files_in_folder(year_folder_id)
        except Exception as e:
            logger.error(f'Could not fetch contents for year folder {year}: {e}')
            print(f'[get_trades_report] ERROR fetching year folder {year}: {e}')
            raise

        print(f'[get_trades_report] year={year} has {len(year_files)} files:')
        for f in year_files:
            print(f'  {f["name"]}')

        # For each requested month, find the latest file in that month
        # Filename format: 732385_YYYYMMDD_YYYYMMDD.csv
        # We look at the second date (end of period) to determine the month
        
        latest_files_for_months = {}
        
        for f in year_files:
            name = f['name']
            if not name.endswith('.csv') or '_' not in name:
                print(f'  [skip] {name!r} (not a csv or no underscore)')
                continue
                
            parts = name.replace('.csv', '').split('_')
            if len(parts) < 3:
                print(f'  [skip] {name!r} (only {len(parts)} parts after split)')
                continue
                
            end_date_str = parts[2]
            if len(end_date_str) < 6:
                print(f'  [skip] {name!r} (end_date_str={end_date_str!r} too short)')
                continue
                
            file_year = end_date_str[:4]
            file_month = end_date_str[4:6]

            print(f'  [parse] {name!r} -> end_date={end_date_str}, file_year={file_year}, file_month={file_month}')
            
            if file_year == year and file_month in months:
                if file_month not in latest_files_for_months or end_date_str > latest_files_for_months[file_month]['end_date']:
                    latest_files_for_months[file_month] = {
                        'id': f['id'],
                        'end_date': end_date_str
                    }
                    print(f'  [match] Selecting {name!r} as latest for month {file_month}')
            else:
                print(f'  [no match] file_year={file_year!r} vs year={year!r}, file_month={file_month!r} vs months={months}')

        print(f'[get_trades_report] latest_files_for_months={latest_files_for_months}')

        def read_trade_month(item):
            month, info = item
            f_data = Drive.download_file(file_id=info['id'], parse=True)
            if not f_data:
                return None
            df = pd.DataFrame(f_data)
            df['Year'] = year
            df['Month'] = month
            return df

        # A failed month must not turn into a successful partial-period total.
        for frame in read_report_files(list(latest_files_for_months.items()), read_trade_month):
            if frame is not None:
                all_trades_dfs.append(frame)

    if not all_trades_dfs:
        return []

    # Combine all dataframes
    combined_trades_df = pd.concat(all_trades_dfs, ignore_index=True)
    
    # Optional: ensure Year and Month columns are at the front for visibility
    cols = ['Year', 'Month'] + [c for c in combined_trades_df.columns if c not in ['Year', 'Month']]
    combined_trades_df = combined_trades_df[cols]
    
    # Sort by Year, Month, and any relevant date column if present (e.g., 'TradeDate' or 'ExecutionTime')
    sort_cols = ['Year', 'Month']
    if 'TradeDate' in combined_trades_df.columns:
        sort_cols.append('TradeDate')
    elif 'Date' in combined_trades_df.columns:
        sort_cols.append('Date')
        
    combined_trades_df = combined_trades_df.sort_values(by=sort_cols, ascending=[False, False] + [False] * (len(sort_cols)-2))

    combined_trades_df = combined_trades_df.fillna('')
    return combined_trades_df.to_dict(orient='records')

@handle_exception
def get_nav_report_monthly(years: list, months: list):
    """
    Extract NAV information grouped by year and month,
    keeping only the latest file available for each month.

    :param years: List of years to include
    :param months: List of months to include (as strings, e.g., '01', '02')
    :return: List of NAV records
    """
    nav_root_folder_id = '1WgYA-Q9mnPYrbbLfYLuJZwUIWBYjiD4c'

    years = [str(y).strip() for y in years if str(y).strip()]
    months = [str(m).strip().zfill(2) for m in months if str(m).strip()]

    # If no months are provided, include all months in each requested year.
    if not months:
        months = [str(m).zfill(2) for m in range(1, 13)]

    try:
        root_contents = Drive.get_files_in_folder(nav_root_folder_id)
    except Exception as e:
        logger.error(f'Could not fetch contents of nav root folder: {nav_root_folder_id}: {e}')
        return []

    all_nav_dfs = []
    files_in_root = [f for f in root_contents if f['mimeType'] != 'application/vnd.google-apps.folder']

    # NAV files are expected at the root with names like 734782_yyyymmdd.csv.
    if files_in_root:
        latest_files_for_period = {}

        for f in files_in_root:
            name = f['name']

            date_match = re.search(r'_(\d{8})(?:\D|$)', name)
            if date_match:
                end_date_str = date_match.group(1)
            else:
                date_matches = re.findall(r'(\d{8})', name)
                if date_matches:
                    end_date_str = date_matches[-1]
                else:
                    end_date_str = datetime.fromisoformat(f['modifiedTime'].replace('Z', '+00:00')).strftime('%Y%m%d')

            file_year = end_date_str[:4]
            file_month = end_date_str[4:6]

            if file_year in years and file_month in months:
                key = (file_year, file_month)
                if key not in latest_files_for_period or end_date_str > latest_files_for_period[key]['end_date']:
                    latest_files_for_period[key] = {
                        'id': f['id'],
                        'end_date': end_date_str
                    }

        for (year, month), info in latest_files_for_period.items():
            try:
                nav_data = Drive.download_file(file_id=info['id'], parse=True)
                if nav_data:
                    df = pd.DataFrame(nav_data)
                    df['Year'] = year
                    df['Month'] = month
                    df['ReportDate'] = info['end_date']
                    all_nav_dfs.append(df)
            except Exception as e:
                logger.error(f'Error downloading/parsing nav file for {year}-{month}: {e}')
    else:
        # Fallback for structures that keep NAV files under year subfolders.
        year_folders = {
            f['name']: f['id']
            for f in root_contents
            if f['mimeType'] == 'application/vnd.google-apps.folder'
        }

        for year in years:
            if year not in year_folders:
                logger.warning(f'Year folder {year} not found in nav root folder.')
                continue

            year_folder_id = year_folders[year]

            try:
                year_files = Drive.get_files_in_folder(year_folder_id)
            except Exception as e:
                logger.error(f'Could not fetch contents for nav year folder {year}: {e}')
                continue

            latest_files_for_months = {}

            for f in year_files:
                if f['mimeType'] == 'application/vnd.google-apps.folder':
                    continue

                name = f['name']
                date_matches = re.findall(r'(\d{8})', name)
                if date_matches:
                    end_date_str = date_matches[-1]
                else:
                    end_date_str = datetime.fromisoformat(f['modifiedTime'].replace('Z', '+00:00')).strftime('%Y%m%d')

                file_year = end_date_str[:4]
                file_month = end_date_str[4:6]

                if file_year == year and file_month in months:
                    if file_month not in latest_files_for_months or end_date_str > latest_files_for_months[file_month]['end_date']:
                        latest_files_for_months[file_month] = {
                            'id': f['id'],
                            'end_date': end_date_str
                        }

            for month, info in latest_files_for_months.items():
                try:
                    nav_data = Drive.download_file(file_id=info['id'], parse=True)
                    if nav_data:
                        df = pd.DataFrame(nav_data)
                        df['Year'] = year
                        df['Month'] = month
                        df['ReportDate'] = info['end_date']
                        all_nav_dfs.append(df)
                except Exception as e:
                    logger.error(f'Error downloading/parsing nav file for {year}-{month}: {e}')

    if not all_nav_dfs:
        return []

    combined_nav_df = pd.concat(all_nav_dfs, ignore_index=True)
    combined_nav_df = combined_nav_df.fillna('')

    return combined_nav_df.to_dict(orient='records')

"""
HARDCODED BACKUPS
"""

@handle_exception
def get_brokerage_commissions():
    """
    Get the brokerage commissions report.
    
    :return: Response object with brokerage commissions report or error message
    """
    brokerage_commissions_root_folder_id = '1s1s6p0tcr3uw-AyHoVO68wDne586ukkY'
    files = Drive.get_files_in_folder(brokerage_commissions_root_folder_id)
    most_recent_file = Drive.get_most_recent_file(files)
    brokerage_commissions = Drive.download_file(file_id=most_recent_file['id'], parse=True)
    brokerage_commissions = _extract_named_sheet_rows(brokerage_commissions, 'brokerage')
    logger.info(f'Brokerage commissions report loaded with {len(brokerage_commissions)} rows')
    return _stringify_dict_keys(brokerage_commissions)

@handle_exception
def get_management_commissions():
    """
    Get the management commissions report.
    
    :return: Response object with management commissions report or error message
    """
    management_commissions_root_folder_id = '1J4M5ppbt0CZzgQ88woKmuoRunLxgdQ-I'
    files = Drive.get_files_in_folder(management_commissions_root_folder_id)
    most_recent_file = Drive.get_most_recent_file(files)
    management_commissions = Drive.download_file(file_id=most_recent_file['id'], parse=True)
    management_commissions = _extract_named_sheet_rows(management_commissions, 'management commissions')
    logger.info(f'Management commissions report loaded with {len(management_commissions)} rows')
    return _stringify_dict_keys(management_commissions)

@handle_exception
def get_ending_balances_from_statements():
    """
    Build ending balances directly from monthly activity statement CSV files.
    
    :return: Response object with ending balances from statements report or error message
    """
    statement_files = []
    seen_account_months = set()
    months_by_account = {account: set() for account in activity_statement_folders}

    for account, folder_id in activity_statement_folders.items():
        files = Drive.get_files_in_folder(folder_id) or []
        filename_pattern = re.compile(rf'{re.escape(account)}_(\d{{4}})(\d{{2}})\.csv', re.IGNORECASE)

        for file_info in files:
            match = filename_pattern.fullmatch(file_info.get('name', ''))
            if not match:
                continue

            account_month = (account, match.group(1), match.group(2))
            if account_month in seen_account_months:
                raise ServiceError(
                    f'Multiple activity statement CSV files found for {account} {match.group(1)}-{match.group(2)}',
                    status_code=500,
                )

            seen_account_months.add(account_month)
            months_by_account[account].add((match.group(1), match.group(2)))
            statement_files.append((*account_month, file_info))

    missing_accounts = [account for account, months in months_by_account.items() if not months]
    if missing_accounts:
        raise ServiceError(
            f'No monthly activity statement CSV files found for: {", ".join(missing_accounts)}',
            status_code=404,
        )

    statement_files.sort(key=lambda item: (item[1], item[2], item[0]))
    cache_key = tuple(
        (account, year, month, file_info['id'], file_info.get('modifiedTime'))
        for account, year, month, file_info in statement_files
    )

    with _ending_balances_cache_lock:
        if _ending_balances_cache['key'] == cache_key:
            return _ending_balances_cache['rows']

        def read_statement(item):
            account, year, month, file_info = item
            file_cache_key = (account, year, month, file_info['id'], file_info.get('modifiedTime'))
            statement_rows = _activity_statement_file_cache.get(file_cache_key)
            if statement_rows is None:
                statement_bytes = Drive.download_file(file_id=file_info['id'], parse=False)
                statement_rows = _extract_change_in_nav_rows(statement_bytes, account, year, month)
            return file_cache_key, statement_rows

        ending_balances = []
        current_file_cache = {}
        for file_cache_key, statement_rows in read_report_files(statement_files, read_statement):
            current_file_cache[file_cache_key] = statement_rows
            ending_balances.extend(statement_rows)

        ending_balances = _stringify_dict_keys(ending_balances)
        _activity_statement_file_cache.clear()
        _activity_statement_file_cache.update(current_file_cache)
        _ending_balances_cache['key'] = cache_key
        _ending_balances_cache['rows'] = ending_balances

    logger.info(f'Ending balances from statements report loaded with {len(ending_balances)} rows')
    return ending_balances


@handle_exception
def get_ibkr_statement_summary(accounts=None, years=None, months=None):
    """Read monthly IBKR account-value summaries directly from Drive CSVs."""
    account_filter = set(accounts or activity_statement_folders)
    year_filter = {str(value) for value in (years or [])}
    month_filter = {str(value).zfill(2) for value in (months or [])}
    summaries = []

    for account, folder_id in activity_statement_folders.items():
        if account not in account_filter:
            continue
        files = Drive.get_files_in_folder(folder_id) or []
        pattern = re.compile(rf'{re.escape(account)}_(\d{{4}})(\d{{2}})\.(csv|pdf)', re.IGNORECASE)
        months_in_folder = {}
        for file_info in files:
            match = pattern.fullmatch(file_info.get('name', ''))
            if not match or (year_filter and match.group(1) not in year_filter) or (month_filter and match.group(2) not in month_filter):
                continue
            month_files = months_in_folder.setdefault((match.group(1), match.group(2)), {})
            file_type = match.group(3).lower()
            if file_type in month_files:
                raise ServiceError(f'Duplicate {file_type.upper()} statement for {account} {match.group(1)}-{match.group(2)}', status_code=500)
            month_files[file_type] = file_info

        for (year, month), month_files in months_in_folder.items():
            csv_file = month_files.get('csv')
            pdf_file = month_files.get('pdf')
            eom = f'{year}-{month}-{calendar.monthrange(int(year), int(month))[1]:02d}'
            if csv_file is None:
                summaries.append({
                    'account': account, 'year': int(year), 'month': int(month), 'eom': eom,
                    'starting_value': None, 'deposits_withdrawals': None, 'dividends': None,
                    'interest': None, 'other_fees': None, 'ending_value': None,
                    'credits': None, 'debits': None, 'cash_opening_balance': None,
                    'cash_closing_balance': None, 'cash_reconciliation_difference': None,
                    'cash_reconciled': False, 'source_file_id': None,
                    'source_file_name': None, 'pdf_file_id': pdf_file['id'],
                    'pdf_file_name': pdf_file.get('name'), 'statement_complete': False,
                })
                continue

            rows = _read_statement_csv(csv_file['id'], csv_file.get('modifiedTime'))
            _validate_statement_account(rows, account, year, month)
            sections = {}
            active_account = None
            for row in rows:
                if len(row) >= 4 and row[:3] == ['Account Information', 'Data', 'Account']:
                    active_account = row[3].strip().split()[0] if row[3].strip() else None
                if active_account == account and len(row) >= 4 and row[:2] == ['Change in NAV', 'Data'] and row[2].strip():
                    sections[row[2].strip()] = _parse_amount(row[3], account, year, month, row[2])
            if 'Starting Value' not in sections or 'Ending Value' not in sections:
                raise ServiceError(f'Change in NAV summary not found for {account} {year}-{month}', status_code=500)

            credits = 0.0
            debits = 0.0
            cash_opening_balance = None
            cash_closing_balance = None
            statement_debits = None
            statement_credits = None
            for row, columns in _base_currency_funds_rows(rows, account, year, month):
                description = row[columns['Description']].strip()
                if description == 'Opening Balance':
                    cash_opening_balance = _parse_amount(row[columns['Balance']], account, year, month, description)
                elif description == 'Closing Balance':
                    cash_closing_balance = _parse_amount(row[columns['Balance']], account, year, month, description)
                    statement_debits = _parse_amount(row[columns['Debit']], account, year, month, description, allow_blank=True)
                    statement_credits = _parse_amount(row[columns['Credit']], account, year, month, description, allow_blank=True)
                else:
                    debits += _parse_amount(row[columns['Debit']], account, year, month, description, allow_blank=True) or 0
                    credits += _parse_amount(row[columns['Credit']], account, year, month, description, allow_blank=True) or 0

            cash_difference = None
            if cash_opening_balance is not None and cash_closing_balance is not None:
                cash_difference = cash_opening_balance + credits + debits - cash_closing_balance

            summaries.append({
                'account': account,
                'year': int(year),
                'month': int(month),
                'starting_value': sections['Starting Value'],
                'deposits_withdrawals': sections.get('Deposits & Withdrawals'),
                'dividends': sections.get('Dividends'),
                'interest': sections.get('Interest'),
                'other_fees': sections.get('Other Fees'),
                'ending_value': sections['Ending Value'],
                'credits': credits,
                'debits': debits,
                'cash_opening_balance': cash_opening_balance,
                'cash_closing_balance': cash_closing_balance,
                'cash_reconciliation_difference': cash_difference,
                'statement_debits': statement_debits,
                'statement_credits': statement_credits,
                'cash_reconciled': cash_difference is not None and abs(cash_difference) < 0.01
                    and statement_debits is not None and abs(debits - statement_debits) < 0.01
                    and statement_credits is not None and abs(credits - statement_credits) < 0.01,
                'eom': eom,
                'source_file_id': csv_file['id'],
                'source_file_name': csv_file.get('name'),
                'pdf_file_id': pdf_file.get('id') if pdf_file else None,
                'pdf_file_name': pdf_file.get('name') if pdf_file else None,
                'statement_complete': pdf_file is not None,
            })

    return sorted(summaries, key=lambda row: (row['year'], row['month'], row['account']))


@handle_exception
def get_ibkr_statement_transactions(accounts=None, years=None, months=None):
    """Read IBKR Base Currency Summary transactions directly from Drive CSVs."""
    account_filter = set(accounts or activity_statement_folders)
    year_filter = {str(value) for value in (years or [])}
    month_filter = {str(value).zfill(2) for value in (months or [])}
    transactions = []

    for account, folder_id in activity_statement_folders.items():
        if account not in account_filter:
            continue
        files = Drive.get_files_in_folder(folder_id) or []
        pattern = re.compile(rf'{re.escape(account)}_(\d{{4}})(\d{{2}})\.csv', re.IGNORECASE)

        for file_info in files:
            match = pattern.fullmatch(file_info.get('name', ''))
            if not match or (year_filter and match.group(1) not in year_filter) or (month_filter and match.group(2) not in month_filter):
                continue

            rows = _read_statement_csv(file_info['id'], file_info.get('modifiedTime'))
            _validate_statement_account(rows, account, match.group(1), match.group(2))
            for row, columns in _base_currency_funds_rows(rows, account, match.group(1), match.group(2)):
                description = row[columns['Description']].strip()
                if description in {'Opening Balance', 'Closing Balance'}:
                    continue
                debit = _parse_amount(row[columns['Debit']], account, match.group(1), match.group(2), description, allow_blank=True)
                credit = _parse_amount(row[columns['Credit']], account, match.group(1), match.group(2), description, allow_blank=True)
                if debit is None and credit is None:
                    continue
                transactions.append({
                    'account': account,
                    'year': int(match.group(1)),
                    'month': int(match.group(2)),
                    'transaction_date': row[columns['Report Date']].strip() or None,
                    'settlement_date': row[columns['Activity Date']].strip() or None,
                    'description': description,
                    'transaction_reference': None,
                    'transaction_code': None,
                    'debit': debit or 0,
                    'credit': credit or 0,
                    'amount': (credit or 0) + (debit or 0),
                    'running_balance': _parse_amount(row[columns['Balance']], account, match.group(1), match.group(2), description),
                    'eom': f"{match.group(1)}-{match.group(2)}-{calendar.monthrange(int(match.group(1)), int(match.group(2)))[1]:02d}",
                    'source_file_id': file_info['id'],
                    'source_file_name': file_info.get('name'),
                })

    return sorted(transactions, key=lambda row: (row['transaction_date'] or '', row['account'], row['description']))


def _read_statement_csv(file_id, modified_time=None):
    cache_key = (file_id, modified_time)
    if modified_time is not None:
        with _ibkr_statement_rows_cache_lock:
            cached_rows = _ibkr_statement_rows_cache.get(cache_key)
            if cached_rows is not None:
                return cached_rows
    statement_bytes = Drive.download_file(file_id=file_id, parse=False)
    try:
        statement_text = statement_bytes.decode('utf-8-sig')
    except UnicodeDecodeError:
        statement_text = statement_bytes.decode('latin1')
    rows = list(csv.reader(io.StringIO(statement_text)))
    if modified_time is not None:
        with _ibkr_statement_rows_cache_lock:
            if len(_ibkr_statement_rows_cache) >= 128:
                _ibkr_statement_rows_cache.pop(next(iter(_ibkr_statement_rows_cache)))
            _ibkr_statement_rows_cache[cache_key] = rows
    return rows


def _base_currency_funds_rows(rows, account, year, month):
    required = {'Currency', 'Report Date', 'Activity Date', 'Description', 'Debit', 'Credit', 'Balance'}
    columns = None
    active_account = None
    for row in rows:
        if len(row) >= 4 and row[:3] == ['Account Information', 'Data', 'Account']:
            active_account = row[3].strip().split()[0] if row[3].strip() else None
        if active_account != account:
            continue
        if row[:2] == ['Statement of Funds', 'Header']:
            columns = {name.strip(): index for index, name in enumerate(row)}
            if not required.issubset(columns):
                raise ServiceError(f'Statement of Funds columns missing for {account} {year}-{month}', status_code=500)
        elif row[:2] == ['Statement of Funds', 'Data']:
            if columns is None or len(row) <= max(columns[name] for name in required):
                raise ServiceError(f'Statement of Funds row is incomplete for {account} {year}-{month}', status_code=500)
            if row[columns['Currency']].strip() != 'Base Currency Summary':
                continue
            if not row[columns['Description']].strip() and 'Account' in columns and row[columns['Account']].strip() in {'Opening Balance', 'Closing Balance'}:
                row = list(row)
                row[columns['Description']] = row[columns['Account']]
            yield row, columns
    if columns is None:
        raise ServiceError(f'Statement of Funds columns missing for {account} {year}-{month}', status_code=500)


def _validate_statement_account(rows, expected_account, year, month):
    account = next((row[3].strip().split()[0] for row in rows
                    if len(row) >= 4 and row[0].strip() == 'Account Information'
                    and row[1].strip() == 'Data' and row[2].strip() == 'Account'), None)
    if account != expected_account:
        raise ServiceError(f'Statement account mismatch for {expected_account} {year}-{month}: {account or "missing"}', status_code=500)


def _parse_amount(raw_value, account, year, month, label, allow_blank=False):
    value = (raw_value or '').strip().replace(',', '')
    if not value or value == '-':
        if allow_blank:
            return None
        raise ServiceError(f'Missing amount for {account} {year}-{month} {label}', status_code=500)
    try:
        return float(value)
    except ValueError as exc:
        raise ServiceError(f'Invalid amount for {account} {year}-{month} {label}: {raw_value}', status_code=500) from exc

def _extract_change_in_nav_rows(statement_bytes, expected_account, year, month):
    """Normalize one account's Change in NAV section from an IBKR statement."""
    try:
        statement_text = statement_bytes.decode('utf-8-sig')
    except UnicodeDecodeError:
        statement_text = statement_bytes.decode('latin1')

    current_account = None
    end_of_month = pd.Timestamp(int(year), int(month), 1) + pd.offsets.MonthEnd(0)
    ending_balances = []

    for row in csv.reader(io.StringIO(statement_text)):
        if len(row) >= 4 and row[0].strip() == 'Account Information' and row[1].strip() == 'Data':
            if row[2].strip() == 'Account':
                account_parts = row[3].strip().split()
                current_account = account_parts[0] if account_parts else None
            continue

        if current_account != expected_account:
            continue
        if len(row) < 4 or row[0].strip() != 'Change in NAV' or row[1].strip() != 'Data':
            continue

        description = row[2].strip()
        raw_amount = row[3].strip()
        if not description or not raw_amount:
            continue

        try:
            amount = float(raw_amount.replace(',', ''))
        except ValueError as exc:
            raise ServiceError(
                f'Invalid Change in NAV amount for {expected_account} {year}-{month} '
                f'{description}: {raw_amount}',
                status_code=500,
            ) from exc

        ending_balances.append({
            'Date': end_of_month,
            'Description': description,
            'Amount': amount,
            'Account': expected_account,
            'EOM': end_of_month,
        })

    if not ending_balances:
        raise ServiceError(
            f'Change in NAV data not found for {expected_account} {year}-{month}',
            status_code=500,
        )

    return ending_balances

def _extract_named_sheet_rows(rows, expected_sheet_name):
    """
    Return only rows from one Excel worksheet (case-insensitive), removing helper metadata.
    """
    if not isinstance(rows, list):
        raise ServiceError(f'Unexpected report format for sheet "{expected_sheet_name}"', status_code=500)

    normalized_expected = expected_sheet_name.strip().lower()
    filtered_rows = []

    for row in rows:
        if not isinstance(row, dict):
            continue

        sheet_name = str(row.get('sheet_name', '')).strip().lower()
        if sheet_name != normalized_expected:
            continue

        clean_row = {k: v for k, v in row.items() if k != 'sheet_name'}
        filtered_rows.append(clean_row)

    if not filtered_rows:
        raise ServiceError(f'Sheet "{expected_sheet_name}" not found in latest report', status_code=404)

    return filtered_rows

def _stringify_dict_keys(value):
    """
    Recursively convert payloads to JSON-safe data:
    - dict keys -> strings
    - pandas null-likes (NaT/NaN) -> None
    - timestamps/datetimes -> ISO strings
    """
    if isinstance(value, dict):
        return {str(k): _stringify_dict_keys(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_stringify_dict_keys(item) for item in value]
    if isinstance(value, tuple):
        return [_stringify_dict_keys(item) for item in value]
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    return value

"""
HARDCODED FILES
"""
@handle_exception
def get_proposals_equity_report():
    """
    Get the proposals equity report.
    
    :return: Response object with proposals equity report or error message
    """
    proposals_equity = Drive.export_file(file_id='1AqpIE7LRV40J-Aew5fA-P6gEfji3Yb-Rp5DohI9BQFY', mime_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', parse=True)
    return proposals_equity
