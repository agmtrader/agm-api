from datetime import datetime, date, timedelta
from src.utils.logger import logger
import io
import threading
import pandas as pd
import pandas as pd
import re
import json
import hashlib
import pytz

from src.utils.connectors.drive import GoogleDrive
from src.utils.exception import ServiceError, handle_exception
from src.utils.connectors.ibkr_web_api import IBKRWebAPI

logger.announcement('Initializing Reporting Service', type='info')
Drive = GoogleDrive()
ibkr_web_api = IBKRWebAPI()
_ending_balances_cache = {'key': None, 'rows': None}
_ending_balances_cache_lock = threading.Lock()

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
    files_in_resources_folder = Drive.get_files_in_folder(resources_folder_id)
    clients_file = [client for client in files_in_resources_folder if 'ibkr_clients' in client['name']]
    if len(clients_file) != 1:
        logger.error('Clients file not found or multiple files found')
        raise Exception('Clients file not found or multiple files found')
    clients = Drive.download_file(file_id=clients_file[0]['id'], parse=True)
    return clients

@handle_exception
def get_client_fees_report():
    """
    Get the client fees list.
    
    :return: Response object with clients list or error message
    """
    files_in_resources_folder = Drive.get_files_in_folder(resources_folder_id)
    clients_file = [client for client in files_in_resources_folder if 'ibkr_client_fees' in client['name']]
    if len(clients_file) != 1:
        logger.error('Clients file not found or multiple files found')
        raise Exception('Clients file not found or multiple files found')
    clients = Drive.download_file(file_id=clients_file[0]['id'], parse=True)
    return clients

@handle_exception
def get_nav_report():
    """
    Get the NAV report.
    
    :return: Response object with NAV report or error message
    """
    files_in_resources_folder = Drive.get_files_in_folder(resources_folder_id)
    nav_file = [nav for nav in files_in_resources_folder if 'ibkr_nav' in nav['name']]
    if len(nav_file) != 1:
        logger.error('Nav file not found or multiple files found')
        raise Exception('Nav file not found or multiple files found')
    nav = Drive.download_file(file_id=nav_file[0]['id'], parse=True)
    return nav

@handle_exception
def get_bond_report():
    """
    Get the RTD report.
    
    :return: Response object with RTD report or error message
    """
    files_in_resources_folder = Drive.get_files_in_folder(resources_folder_id)
    rtd_file = [rtd for rtd in files_in_resources_folder if 'ibkr_bonds_snapshot' in rtd['name']]
    if len(rtd_file) != 1:
        logger.error('RTD file not found or multiple files found')
        raise Exception('RTD file not found or multiple files found')
    rtd = Drive.download_file(file_id=rtd_file[0]['id'], parse=True)
    return rtd  

@handle_exception
def get_stocks_report():
    """
    Get the Stocks report.
    
    :return: Response object with RTD report or error message
    """
    files_in_resources_folder = Drive.get_files_in_folder(resources_folder_id)
    rtd_file = [rtd for rtd in files_in_resources_folder if 'ibkr_stocks_snapshot' in rtd['name']]
    if len(rtd_file) != 1:
        logger.error('Stocks file not found or multiple files found')
        raise Exception('Stocks file not found or multiple files found')
    rtd = Drive.download_file(file_id=rtd_file[0]['id'], parse=True)
    return rtd

@handle_exception
def get_etfs_report():
    """
    Get the ETFs report.
    
    :return: Response object with ETFs report or error message
    """
    files_in_resources_folder = Drive.get_files_in_folder(resources_folder_id)
    rtd_file = [rtd for rtd in files_in_resources_folder if 'ibkr_etfs_snapshot' in rtd['name']]
    if len(rtd_file) != 1:
        logger.error('ETFs file not found or multiple files found')
        raise Exception('ETFs file not found or multiple files found')
    rtd = Drive.download_file(file_id=rtd_file[0]['id'], parse=True)
    return rtd

@handle_exception
def get_ust_bond_report():
    """
    Get the UST Bonds report.

    :return: Response object with UST Bonds report or error message
    """
    files_in_resources_folder = Drive.get_files_in_folder(resources_folder_id)
    rtd_file = [rtd for rtd in files_in_resources_folder if 'ibkr_ust_bonds_snapshot' in rtd['name']]
    if len(rtd_file) != 1:
        logger.error('UST Bonds file not found or multiple files found')
        raise Exception('UST Bonds file not found or multiple files found')
    rtd = Drive.download_file(file_id=rtd_file[0]['id'], parse=True)
    return rtd

@handle_exception
def get_open_positions_report():
    """
    Get the open positions report.
    
    :return: Response object with open positions report or error message
    """
    files_in_resources_folder = Drive.get_files_in_folder(resources_folder_id)
    open_positions_file = [open_positions for open_positions in files_in_resources_folder if 'ibkr_open_positions_all' in open_positions['name']]
    if len(open_positions_file) != 1:
        logger.error('Open positions file not found or multiple files found')
        raise Exception('Open positions file not found or multiple files found')
    open_positions = Drive.download_file(file_id=open_positions_file[0]['id'], parse=True)
    return open_positions

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

    added_keys = sorted(set(today_map.keys()) - set(yesterday_map.keys()))
    removed_keys = sorted(set(yesterday_map.keys()) - set(today_map.keys()))

    return {
        'added_count': len(added_keys),
        'removed_count': len(removed_keys),
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
    files_in_resources_folder = Drive.get_files_in_folder(resources_folder_id)
    ibkr_account_details_file = [ibkr_account_details for ibkr_account_details in files_in_resources_folder if 'ibkr_account_details' in ibkr_account_details['name']]
    if len(ibkr_account_details_file) != 1:
        logger.error('IBKR account details file not found or multiple files found')
        raise Exception('IBKR account details file not found or multiple files found')
    ibkr_details = Drive.download_file(file_id=ibkr_account_details_file[0]['id'], parse=True)
    return ibkr_details

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
        return []

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
            continue

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

        # Download and parse each latest file
        for month, info in latest_files_for_months.items():
            try:
                logger.info(f'Downloading trades for {year}-{month} from file {info["end_date"]}')
                print(f'[get_trades_report] Downloading {year}-{month}, file_id={info["id"]}')
                f_data = Drive.download_file(file_id=info['id'], parse=True)
                if f_data:
                    df = pd.DataFrame(f_data)
                    df['Year'] = year
                    df['Month'] = month
                    all_trades_dfs.append(df)
                    print(f'[get_trades_report] Loaded {len(df)} rows for {year}-{month}')
                else:
                    print(f'[get_trades_report] WARNING: empty file for {year}-{month}')
            except Exception as e:
                logger.error(f'Error downloading/parsing file for {year}-{month}: {e}')
                print(f'[get_trades_report] ERROR downloading {year}-{month}: {e}')

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

@handle_exception
def get_monthly_client_fees(years: list, months: list):
    """
    Extract client fees information grouped by year and month,
    keeping only the latest file available for each month.

    :param years: List of years to include
    :param months: List of months to include (as strings, e.g., '01', '02')
    :return: List of client fees records
    """
    client_fees_root_folder_id = '1OnSEo8B2VUF5u-VkhtzZVIzx6ABe_YB7'

    years = [str(y).strip() for y in years if str(y).strip()]
    months = [str(m).strip().zfill(2) for m in months if str(m).strip()]

    # If no months are provided, include all months in each requested year.
    if not months:
        months = [str(m).zfill(2) for m in range(1, 13)]

    try:
        root_contents = Drive.get_files_in_folder(client_fees_root_folder_id)
    except Exception as e:
        logger.error(f'Could not fetch contents of client fees root folder: {client_fees_root_folder_id}: {e}')
        return []

    all_client_fees_dfs = []
    files_in_root = [f for f in root_contents if f['mimeType'] != 'application/vnd.google-apps.folder']

    # Client fees files are expected as files named like 732383_YYYYMMDD_YYYYMMDD.csv.
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
                fees_data = Drive.download_file(file_id=info['id'], parse=True)
                if fees_data:
                    df = pd.DataFrame(fees_data)
                    df['Year'] = year
                    df['Month'] = month
                    df['ReportDate'] = info['end_date']
                    all_client_fees_dfs.append(df)
            except Exception as e:
                logger.error(f'Error downloading/parsing client fees file for {year}-{month}: {e}')
    else:
        # Fallback for structures that keep client fees files under year subfolders.
        year_folders = {
            f['name']: f['id']
            for f in root_contents
            if f['mimeType'] == 'application/vnd.google-apps.folder'
        }

        for year in years:
            if year not in year_folders:
                logger.warning(f'Year folder {year} not found in client fees root folder.')
                continue

            year_folder_id = year_folders[year]

            try:
                year_files = Drive.get_files_in_folder(year_folder_id)
            except Exception as e:
                logger.error(f'Could not fetch contents for client fees year folder {year}: {e}')
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
                    fees_data = Drive.download_file(file_id=info['id'], parse=True)
                    if fees_data:
                        df = pd.DataFrame(fees_data)
                        df['Year'] = year
                        df['Month'] = month
                        df['ReportDate'] = info['end_date']
                        all_client_fees_dfs.append(df)
                except Exception as e:
                    logger.error(f'Error downloading/parsing client fees file for {year}-{month}: {e}')

    if not all_client_fees_dfs:
        return []

    combined_client_fees_df = pd.concat(all_client_fees_dfs, ignore_index=True)
    combined_client_fees_df = combined_client_fees_df.fillna('')

    return combined_client_fees_df.to_dict(orient='records')

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
    Get the multi-account ending balances generated from activity statements.
    
    :return: Response object with ending balances from statements report or error message
    """
    ending_balances_root_folder_id = '1VPEFxk4kRMWTYgj7NbJ3Ytpn2t5AGKpa'
    files = Drive.get_files_in_folder(ending_balances_root_folder_id) or []
    if not files:
        raise ServiceError('Ending balances report not found', status_code=404)

    most_recent_file = Drive.get_most_recent_file(files)
    cache_key = (most_recent_file['id'], most_recent_file.get('modifiedTime'))

    with _ending_balances_cache_lock:
        if _ending_balances_cache['key'] == cache_key:
            return _ending_balances_cache['rows']

        report_bytes = Drive.download_file(file_id=most_recent_file['id'], parse=False)
        ending_balances = _extract_ending_balances_sheet(report_bytes)

        ending_balances = _stringify_dict_keys(ending_balances)
        _ending_balances_cache['key'] = cache_key
        _ending_balances_cache['rows'] = ending_balances

    logger.info(f'Ending balances from statements report loaded with {len(ending_balances)} rows')
    return ending_balances


def _extract_ending_balances_sheet(report_bytes):
    """Read the headerless Ending Balances worksheet into the API row contract."""
    sheets = pd.read_excel(io.BytesIO(report_bytes), sheet_name=None, header=None)
    sheet_name = next((name for name in sheets if name.strip().lower() == 'ending balances'), None)
    if sheet_name is None:
        raise ServiceError('Sheet "Ending Balances" not found in latest report', status_code=404)

    ending_balances = sheets[sheet_name]
    if ending_balances.shape[1] < 5:
        raise ServiceError('Unexpected Ending Balances worksheet format', status_code=500)

    ending_balances = ending_balances.iloc[:, :5].copy()
    ending_balances.columns = ['Date', 'Description', 'Amount', 'Account', 'EOM']
    ending_balances = ending_balances.dropna(how='all')
    ending_balances = ending_balances[
        ~(
            ending_balances['Date'].astype(str).str.strip().str.lower().eq('date')
            & ending_balances['Description'].astype(str).str.strip().str.lower().eq('description')
        )
    ]

    ending_balances = ending_balances.astype(object).where(pd.notna(ending_balances), '')
    return ending_balances.to_dict(orient='records')


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
