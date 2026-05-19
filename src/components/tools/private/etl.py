from pandas.tseries.offsets import BDay
from datetime import datetime
import pandas as pd
import base64
import time
import pytz
import pandas as pd
import requests
import re
import json
import xml.etree.ElementTree as ET
from io import StringIO

from src.utils.connectors.drive import GoogleDrive
from src.utils.connectors.flex_query_api import getFlexQuery
from src.utils.exception import handle_exception
from src.utils.connectors.ibkr_web_api import IBKRWebAPI
from src.components.tools.public.reporting import get_bond_report
from src.utils.logger import logger

logger.announcement('Initializing Reporting Service', type='info')
Drive = GoogleDrive()
ibkr_web_api = IBKRWebAPI()

batch_folder_id = '1N3LwrG7IossvCrrrFufWMb26VOcRxhi8'
resources_folder_id = '18Gtm0jl1HRfb1B_3iGidp9uPvM5ZYhOF'

# Get the current time in CST
cst = pytz.timezone('America/Costa_Rica')
cst_time = datetime.now(cst)
today_date = cst_time.strftime('%Y%m%d%H%M')
yesterday_date = (cst_time - BDay(1)).strftime('%Y%m%d')
first_date = cst_time.replace(day=1).strftime('%Y%m%d')

logger.announcement('Initialized Reporting Service', type='success')

ratings = {
    # S&P Ratings
    "AAA": {"Short-term": "A-1+", "NAIC": 1, "Class1": "Prime", "Class2": "Investment-grade", "Class3": "Investment grade", "Level": 1, "S&P Equivalent": "AAA", "Source": "S&P"},
    "AA+": {"Short-term": "A-1+", "NAIC": 1, "Class1": "High grade", "Class2": "Investment-grade", "Class3": "Investment grade", "Level": 2, "S&P Equivalent": "AA+", "Source": "S&P"},
    "AA": {"Short-term": "A-1+", "NAIC": 1, "Class1": "High grade", "Class2": "Investment-grade", "Class3": "Investment grade", "Level": 3, "S&P Equivalent": "AA", "Source": "S&P"},
    "AA-": {"Short-term": "A-1+", "NAIC": 1, "Class1": "High grade", "Class2": "Investment-grade", "Class3": "Investment grade", "Level": 4, "S&P Equivalent": "AA-", "Source": "S&P"},
    "A+": {"Short-term": "A-1", "NAIC": 1, "Class1": "Upper medium grade", "Class2": "Investment-grade", "Class3": "Investment grade", "Level": 5, "S&P Equivalent": "A+", "Source": "S&P"},
    "A": {"Short-term": "A-1", "NAIC": 1, "Class1": "Upper medium grade", "Class2": "Investment-grade", "Class3": "Investment grade", "Level": 6, "S&P Equivalent": "A", "Source": "S&P"},
    "A-": {"Short-term": "A-2", "NAIC": 1, "Class1": "Upper medium grade", "Class2": "Investment-grade", "Class3": "Investment grade", "Level": 7, "S&P Equivalent": "A-", "Source": "S&P"},
    "BBB+": {"Short-term": "A-2", "NAIC": 2, "Class1": "Lower medium grade", "Class2": "Investment-grade", "Class3": "Investment grade", "Level": 8, "S&P Equivalent": "BBB+", "Source": "S&P"},
    "BBB": {"Short-term": "A-3", "NAIC": 2, "Class1": "Lower medium grade", "Class2": "Investment-grade", "Class3": "Investment grade", "Level": 9, "S&P Equivalent": "BBB", "Source": "S&P"},
    "BBB-": {"Short-term": "B", "NAIC": 3, "Class1": "Lower medium grade", "Class2": "Investment-grade", "Class3": "Investment grade", "Level": 10, "S&P Equivalent": "BBB-", "Source": "S&P"},
    "BB+": {"Short-term": "B", "NAIC": 3, "Class1": "Non-investment grade", "Class2": "Non-investment grade", "Class3": "Non-investment grade", "Level": 11, "S&P Equivalent": "BB+", "Source": "S&P"},
    "BB": {"Short-term": "B", "NAIC": 3, "Class1": "Speculative", "Class2": "AKA high-yield bonds", "Class3": "Non-investment grade", "Level": 12, "S&P Equivalent": "BB", "Source": "S&P"},
    "BB-": {"Short-term": "B", "NAIC": 3, "Class1": "Speculative", "Class2": "AKA junk bonds", "Class3": "Non-investment grade", "Level": 13, "S&P Equivalent": "BB-", "Source": "S&P"},
    "B+": {"Short-term": "B", "NAIC": 4, "Class1": "Highly speculative", "Class2": "AKA junk bonds", "Class3": "Non-investment grade", "Level": 14, "S&P Equivalent": "B+", "Source": "S&P"},
    "B": {"Short-term": "B", "NAIC": 4, "Class1": "Highly speculative", "Class2": "AKA junk bonds", "Class3": "Non-investment grade", "Level": 15, "S&P Equivalent": "B", "Source": "S&P"},
    "B-": {"Short-term": "B", "NAIC": 4, "Class1": "Highly speculative", "Class2": "AKA junk bonds", "Class3": "Non-investment grade", "Level": 16, "S&P Equivalent": "B-", "Source": "S&P"},
    "CCC+": {"Short-term": "C", "NAIC": 5, "Class1": "Substantial risks", "Class2": "AKA junk bonds", "Class3": "Non-investment grade", "Level": 17, "S&P Equivalent": "CCC+", "Source": "S&P"},
    "CCC": {"Short-term": "C", "NAIC": 5, "Class1": "Extremely speculative", "Class2": "AKA junk bonds", "Class3": "Non-investment grade", "Level": 18, "S&P Equivalent": "CCC", "Source": "S&P"},
    "CCC-": {"Short-term": "C", "NAIC": 5, "Class1": "Default imminent with little prospect for recovery", "Class2": "AKA junk bonds", "Class3": "Non-investment grade", "Level": 19, "S&P Equivalent": "CCC-", "Source": "S&P"},
    "CC": {"Short-term": "C", "NAIC": 6, "Class1": "Default imminent with little prospect for recovery", "Class2": "AKA junk bonds", "Class3": "Non-investment grade", "Level": 20, "S&P Equivalent": "CC", "Source": "S&P"},
    "C": {"Short-term": "C", "NAIC": 6, "Class1": "In default", "Class2": "AKA junk bonds", "Class3": "Non-investment grade", "Level": 21, "S&P Equivalent": "C", "Source": "S&P"},
    "D": {"Short-term": "/", "NAIC": 6, "Class1": "In default", "Class2": "AKA junk bonds", "Class3": "Non-investment grade", "Level": 22, "S&P Equivalent": "D", "Source": "S&P"},

    # Moody's Ratings
    "Aaa": {"Short-term": "P-1", "NAIC": 1, "Class1": "Prime", "Class2": "Investment-grade", "Class3": "Investment grade", "Level": 1, "S&P Equivalent": "AAA", "Source": "Moody's"},
    "Aa1": {"Short-term": "P-1", "NAIC": 1, "Class1": "High grade", "Class2": "Investment-grade", "Class3": "Investment grade", "Level": 2, "S&P Equivalent": "AA+", "Source": "Moody's"},
    "Aa2": {"Short-term": "P-1", "NAIC": 1, "Class1": "High grade", "Class2": "Investment-grade", "Class3": "Investment grade", "Level": 3, "S&P Equivalent": "AA", "Source": "Moody's"},
    "Aa3": {"Short-term": "P-1", "NAIC": 1, "Class1": "High grade", "Class2": "Investment-grade", "Class3": "Investment grade", "Level": 4, "S&P Equivalent": "AA-", "Source": "Moody's"},
    "A1": {"Short-term": "P-1", "NAIC": 1, "Class1": "Upper medium grade", "Class2": "Investment-grade", "Class3": "Investment grade", "Level": 5, "S&P Equivalent": "A+", "Source": "Moody's"},
    "A2": {"Short-term": "P-1", "NAIC": 1, "Class1": "Upper medium grade", "Class2": "Investment-grade", "Class3": "Investment grade", "Level": 6, "S&P Equivalent": "A", "Source": "Moody's"},
    "A3": {"Short-term": "P-2", "NAIC": 1, "Class1": "Upper medium grade", "Class2": "Investment-grade", "Class3": "Investment grade", "Level": 7, "S&P Equivalent": "A-", "Source": "Moody's"},
    "Baa1": {"Short-term": "P-2", "NAIC": 2, "Class1": "Lower medium grade", "Class2": "Investment-grade", "Class3": "Investment grade", "Level": 8, "S&P Equivalent": "BBB+", "Source": "Moody's"},
    "Baa2": {"Short-term": "P-3", "NAIC": 2, "Class1": "Lower medium grade", "Class2": "Investment-grade", "Class3": "Investment grade", "Level": 9, "S&P Equivalent": "BBB", "Source": "Moody's"},
    "Baa3": {"Short-term": "P-3", "NAIC": 3, "Class1": "Lower medium grade", "Class2": "Investment-grade", "Class3": "Investment grade", "Level": 10, "S&P Equivalent": "BBB-", "Source": "Moody's"},
    "Ba1": {"Short-term": "Not prime", "NAIC": 3, "Class1": "Non-investment grade", "Class2": "Non-investment grade", "Class3": "Non-investment grade", "Level": 11, "S&P Equivalent": "BB+", "Source": "Moody's"},
    "Ba2": {"Short-term": "Not prime", "NAIC": 3, "Class1": "Speculative", "Class2": "AKA high-yield bonds", "Class3": "Non-investment grade", "Level": 12, "S&P Equivalent": "BB", "Source": "Moody's"},
    "Ba3": {"Short-term": "Not prime", "NAIC": 4, "Class1": "Speculative", "Class2": "AKA junk bonds", "Class3": "Non-investment grade", "Level": 13, "S&P Equivalent": "BB-", "Source": "Moody's"},
    "B1": {"Short-term": "Not prime", "NAIC": 4, "Class1": "Highly speculative", "Class2": "AKA junk bonds", "Class3": "Non-investment grade", "Level": 14, "S&P Equivalent": "B+", "Source": "Moody's"},
    "B2": {"Short-term": "Not prime", "NAIC": 4, "Class1": "Highly speculative", "Class2": "AKA junk bonds", "Class3": "Non-investment grade", "Level": 15, "S&P Equivalent": "B", "Source": "Moody's"},
    "B3": {"Short-term": "Not prime", "NAIC": 4, "Class1": "Highly speculative", "Class2": "AKA junk bonds", "Class3": "Non-investment grade", "Level": 16, "S&P Equivalent": "B-", "Source": "Moody's"},
    "Caa1": {"Short-term": "Not prime", "NAIC": 5, "Class1": "Substantial risks", "Class2": "AKA junk bonds", "Class3": "Non-investment grade", "Level": 17, "S&P Equivalent": "CCC+", "Source": "Moody's"},
    "Caa2": {"Short-term": "Not prime", "NAIC": 5, "Class1": "Extremely speculative", "Class2": "AKA junk bonds", "Class3": "Non-investment grade", "Level": 18, "S&P Equivalent": "CCC", "Source": "Moody's"},
    "Caa3": {"Short-term": "Not prime", "NAIC": 5, "Class1": "Default imminent with little prospect for recovery", "Class2": "AKA junk bonds", "Class3": "Non-investment grade", "Level": 19, "S&P Equivalent": "CCC-", "Source": "Moody's"},
    "Ca": {"Short-term": "Not prime", "NAIC": 6, "Class1": "Default imminent with little prospect for recovery", "Class2": "AKA junk bonds", "Class3": "Non-investment grade", "Level": 20, "S&P Equivalent": "CC", "Source": "Moody's"},
    "C": {"Short-term": "Not prime", "NAIC": 6, "Class1": "In default", "Class2": "AKA junk bonds", "Class3": "Non-investment grade", "Level": 21, "S&P Equivalent": "C", "Source": "Moody's"},
    "D": {"Short-term": "Not prime", "NAIC": 6, "Class1": "In default", "Class2": "AKA junk bonds", "Class3": "Non-investment grade", "Level": 22, "S&P Equivalent": "D", "Source": "Moody's"},
}

"""
ETL PIPELINE
"""
@handle_exception
def run_pipeline(pipeline_name_or_config):
    etl_config = pipeline_name_or_config
    if isinstance(pipeline_name_or_config, str):
        etl_config = _get_etl_config_by_name(pipeline_name_or_config)
        if etl_config is None:
            raise ValueError(f"Pipeline '{pipeline_name_or_config}' not found.")

    return _run_steps([
        {'name': 'extract', 'fn': lambda: extract_data(etl_config)},
        {'name': 'backup', 'fn': lambda: backup_data(etl_config)},
        {'name': 'transform', 'fn': lambda: transform_data(etl_config)},
    ])

"""
EXTRACT
"""
def extract_data(etl_config) -> dict:
    logger.announcement('Extracting information for reports.', type='info')

    steps = []
    for file_config in etl_config.get('files', []):
        step_name = file_config.get('name', 'unknown')
        extract_func = file_config.get('extract_func')
        if extract_func is None:
            steps.append({'name': step_name, 'status': 'skipped'})
            continue
        try:
            extract_func(file_config)
            steps.append({'name': step_name, 'status': 'success'})
        except Exception as e:
            logger.error(f'Error during {step_name}: {e}')
            steps.append({'name': step_name, 'status': 'failed', 'error': str(e)})

    failed_steps = [step for step in steps if step.get('status') != 'success']
    overall_status = 'success' if not failed_steps else 'partial'

    logger.announcement('Information successfully extracted for reports.', type='success')
    return {
        'status': overall_status,
        'summary': {
            'total': len(steps),
            'successful': len([s for s in steps if s.get('status') == 'success']),
            'skipped': len([s for s in steps if s.get('status') == 'skipped']),
            'failed': len(failed_steps)
        },
        'steps': steps
    }

# Clients
def extract_flex_query(config):
    """
    Extract a single flex query.
    
    :return: JSON overview with flex query fetch/upload status
    """
    query_id = config['name']
    logger.announcement(f'Fetching Flex Query {query_id}.', type='info')
    max_attempts = 5
    retry_sleep_seconds = 5
    attempt = 1
    while attempt <= max_attempts:
        try:
            flex_query_data = getFlexQuery(query_id)
            break
        except Exception as e:
            if attempt == max_attempts:
                logger.error(f'Error fetching Flex Query for {query_id} after {max_attempts} attempts')
                raise
            logger.warning(
                f'Error fetching Flex Query for {query_id}. '
                f'Retrying in {retry_sleep_seconds}s ({attempt}/{max_attempts})'
            )
            time.sleep(retry_sleep_seconds)
            attempt += 1

    logger.announcement(f'Uploading Flex Query {query_id} to batch folder.', type='info')
    Drive.upload_file(
        file_name=config['backup_name'],
        mime_type='text/csv',
        file_data=(
            flex_query_data.to_dict(orient='records')
            if isinstance(flex_query_data, pd.DataFrame)
            else flex_query_data
        ),
        parent_folder_id=batch_folder_id
    )
    logger.announcement(f'Flex Query {query_id} uploaded.', type='success')
    time.sleep(2)

    return {'name': query_id, 'status': 'success'}

def extract_ofac_sdn_list(config=None):
    logger.announcement('Extracting OFAC SDN list.', type='info')
    sdn_url = 'https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/SDN.CSV'
    consolidated_url = 'https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/CONS_PRIM.CSV'

    sdn_data = requests.get(sdn_url).content
    consolidated_data = requests.get(consolidated_url).content
    csv_data = b'entity_number,name,type,program,title,call_sign,vessel_type,tonnage,gross_registered_tonnage,vessel_flag,vessel_owner,more_info\n' + sdn_data + consolidated_data

    df = pd.read_csv(StringIO(csv_data.decode('utf-8')))
    df['name'] = df['name'].astype(str)

    # If the column 1 is Individual or individual, then change the name to be 
    for index, row in df.iterrows():
        if row['type'] == 'Individual' or row['type'] == 'individual':
            df.at[index, 'name'] = row['name'].split(',')[1] + ' ' + row['name'].split(',')[0]
    
    Drive.upload_file(file_name='ofac_sdn_list.csv', mime_type='text/csv', file_data=df.to_dict(orient='records'), parent_folder_id=batch_folder_id)
    logger.announcement('OFAC SDN list extracted and uploaded to batch folder.', type='success')

def extract_uk_sanctions_list(config=None):
    logger.announcement('Extracting UK sanctions list.', type='info')
    uk_sanctions_url = 'https://sanctionslist.fcdo.gov.uk/docs/UK-Sanctions-List.csv'

    uk_sanctions_data = requests.get(uk_sanctions_url)
    uk_sanctions_data.raise_for_status()
    raw_text = uk_sanctions_data.content.decode('utf-8-sig')
    lines = raw_text.splitlines()

    header_idx = None
    for i, line in enumerate(lines):
        if line.startswith('Last Updated,Unique ID,'):
            header_idx = i
            break

    if header_idx is None:
        logger.error('Could not find UK sanctions CSV header row.')
        raise Exception('Could not find UK sanctions CSV header row.')

    csv_text = '\n'.join(lines[header_idx:])
    df = pd.read_csv(StringIO(csv_text), dtype=str, keep_default_na=False)
    df = df.fillna('')

    Drive.upload_file(
        file_name='uk_sanctions_list.csv',
        mime_type='text/csv',
        file_data=df.to_dict(orient='records'),
        parent_folder_id=batch_folder_id
    )
    logger.announcement('UK sanctions list extracted and uploaded to batch folder.', type='success')

def extract_un_sanctions_list(config=None):
    logger.announcement('Extracting UN sanctions list.', type='info')

    un_sanctions_response = requests.get('https://scsanctions.un.org/resources/xml/en/name/consolidated.xml?_gl=1*b6x5x9*_ga*MTM1Mzg4ODEzNS4xNzc3NjU0MjY4*_ga_TK9BQL5X7Z*czE3Nzc2NTQyNjgkbzEkZzEkdDE3Nzc2NTU1NzEkajYwJGwwJGgw')
    un_sanctions_response.raise_for_status()
    root = ET.fromstring(un_sanctions_response.content)

    def _clean_text(value):
        if value is None:
            return ''
        return str(value).strip()

    def _join_name_parts(element, parts):
        values = [_clean_text(element.findtext(part)) for part in parts]
        return ' '.join([value for value in values if value != ''])

    un_rows = []

    for individual in root.findall('./INDIVIDUALS/INDIVIDUAL'):
        primary_name = _join_name_parts(individual, ['FIRST_NAME', 'SECOND_NAME', 'THIRD_NAME', 'FOURTH_NAME'])
        if primary_name == '':
            primary_name = _clean_text(individual.findtext('NAME_ORIGINAL_SCRIPT'))

        aliases = []
        for alias in individual.findall('INDIVIDUAL_ALIAS'):
            alias_name = _clean_text(alias.findtext('ALIAS_NAME'))
            if alias_name != '':
                aliases.append(alias_name)

        un_rows.append({
            'name': primary_name,
            'entity_number': _clean_text(individual.findtext('REFERENCE_NUMBER')),
            'type': 'Individual',
            'program': _clean_text(individual.findtext('UN_LIST_TYPE')),
            'title': _clean_text(individual.findtext('DESIGNATION/VALUE')),
            'similarity': '',
            'call_sign': '',
            'vessel_type': '',
            'tonnage': '',
            'gross_registered_tonnage': '',
            'vessel_flag': '',
            'vessel_owner': '',
            'more_info': _clean_text(individual.findtext('COMMENTS1')),
            'aliases': '|'.join(aliases),
            'source': 'UN'
        })

    for entity in root.findall('./ENTITIES/ENTITY'):
        primary_name = _join_name_parts(entity, ['FIRST_NAME', 'SECOND_NAME', 'THIRD_NAME', 'FOURTH_NAME'])
        aliases = []
        for alias in entity.findall('ENTITY_ALIAS'):
            alias_name = _clean_text(alias.findtext('ALIAS_NAME'))
            if alias_name != '':
                aliases.append(alias_name)

        un_rows.append({
            'name': primary_name,
            'entity_number': _clean_text(entity.findtext('REFERENCE_NUMBER')),
            'type': 'Entity',
            'program': _clean_text(entity.findtext('UN_LIST_TYPE')),
            'title': '',
            'similarity': '',
            'call_sign': '',
            'vessel_type': '',
            'tonnage': '',
            'gross_registered_tonnage': '',
            'vessel_flag': '',
            'vessel_owner': '',
            'more_info': _clean_text(entity.findtext('COMMENTS1')),
            'aliases': '|'.join(aliases),
            'source': 'UN'
        })

    df = pd.DataFrame(un_rows).fillna('')

    Drive.upload_file(
        file_name='un_sanctions_list.csv',
        mime_type='text/csv',
        file_data=df.to_dict(orient='records'),
        parent_folder_id=batch_folder_id
    )
    logger.announcement('UN sanctions list extracted and uploaded to batch folder.', type='success')
    return {'status': 'success'}

def _append_missing_account_details(details: list):

    SKIP_ACCOUNT_DETAILS_ACCOUNT_IDS = {
        'Paper Trading account', 'U11255020', 'JJJJ', 'mgflt1245', '.', 'U5667692', 'U6545558', 'U6601533',
        'U6481241', 'U6514132', 'U6456839', 'U6343218', 'U6299911', 'U6201107', 'U6192519', '?', 'U5974991',
        'U6017743', 'U5961489', 'U5959931', 'U5862172', 'U5885640', 'U5704986', 'U5667734', 'U5549845',
        'U5360244', 'U5360311', 'U5312423', 'U5224474', 'U5212993', 'U5209932', 'U4993318', 'U5176479',
        'U5866637', 'U4911438', 'U4900285', 'U4838642', 'U5213625', 'U4735325', 'U4668914', 'U4549008',
        'U4470712', 'U4434708', 'U4346346', 'U4273862', 'U4279195', 'U4319961', 'U4218638', 'U4259153',
        'U4127849', 'U4196108', 'U4055337', 'U4083153', 'U3965484', 'U4185115', 'U4470860', 'U4605137',
        'U4625417', 'U4656580', 'U3652992', 'U3633092', 'U3604455', 'U3511434', 'U3511297', 'U3496363',
        'U3495597', 'U3545775', 'U3535501', 'U3429534', 'U3428791', 'U3464930', 'U3372072', 'U3304536',
        'U3314477', 'U6077094', 'U3208714', 'U3228034', 'U3315369', 'U3412654', 'U3419606', 'U3528304',
        'U3604393', 'U3677610', 'U3776509', 'U3224638', 'U3171464', 'U2676460', 'U4714674', 'U2639635',
        'U2478829', 'U2440709', 'U2447095', 'U2479810', 'U2482465', 'U2677556', 'U2738060', 'U2940074',
        'U3034848', 'U3160536', 'U3240192', 'U3247007', 'U2245491', 'U2169126', 'U2169127', 'U2358621',
        'U2112241', 'U2014203', 'U1960643', 'U1809703', 'U1763844', 'U2798255', 'U1753236', 'U1558124',
        'U1512073', 'U1224910', 'U1213466', 'U1206083', 'U1192065', 'U1180647', 'U1161945', 'U1139038',
        'U1117382', 'U1114073', 'U7201776', 'U8431354', 'U6061707', 'U4892747', 'U4127415', 'U3320909',
        'U1037726', 'U928543', 'U918392', 'U877620', 'U758608', 'U743013', 'U528111', 'U471311', 'U450281',
        'U436576', 'U401595', 'U13388281', 'U2573636', 'U7115856', 'U25243779', 'U13321987', 'U6774207',
        'U6978407', 'U7107190', 'U7130162', 'U7254887', 'U7250030', 'U7442191', 'U7558648', 'U7585547',
        'U7692759', 'U7662396', 'U7761674', 'U7779413', 'U7849662', 'U7762995', 'U7811476', 'U7954199',
        'U7968914', 'U9151583', 'U9074670', 'U9074960', 'U8928849', 'U8944802', 'U8477223', 'U8497149',
        'U8455007', 'U8334049', 'U8210758', 'U8437534', 'U9324890', 'U9858733', 'U10553677', 'U10509335',
        'U10748811', 'U10763225', 'U10770438', 'U10977992', 'U10955481', 'U10476251', 'U10545876',
        'U10565675', 'U10790476', 'U10627439', 'U20020075', 'U13708322'
    }

    
    from src.components.clients.accounts import read_accounts, read_account_details
    accounts = read_accounts({})
    detail_account_ids = {
        str(detail.get('account', {}).get('accountId')).strip()
        for detail in details
        if (
            isinstance(detail, dict)
            and isinstance(detail.get('account'), dict)
            and detail.get('account', {}).get('accountId')
        )
    }

    missing_accounts = [
        account for account in accounts
        if str(account.get('ibkr_account_number') or '').strip()
        and str(account.get('ibkr_account_number')).strip() not in detail_account_ids
    ]

    logger.info(f'Found {len(missing_accounts)} accounts missing account details.')
    for account in missing_accounts:
        account_id = str(account.get('ibkr_account_number') or '').strip()
        if not account_id:
            continue
        if account_id in SKIP_ACCOUNT_DETAILS_ACCOUNT_IDS:
            continue

        master_account = account.get('master_account')

        if master_account is None:
            logger.warning(f'Skipping account {account_id}: missing master_account.')
            print(f'[account_details_failed] {account_id} (missing master_account)')
            continue

        try:
            new_details = read_account_details(account_id=account_id, master_account=master_account)
            if new_details:
                details.append(new_details)
        except Exception as e:
            logger.error(f'Error fetching details for account {account_id}: {e}')
            print(f'[account_details_failed] {account_id}')
            continue

    return details

def extract_account_details_backup(config=None):
    logger.announcement('Extracting account details backup.', type='info')
    account_details_config = config or _get_file_config('account_details', [clients_etl])
    if account_details_config is None:
        logger.warning('Account details config not found. Skipping account details extraction.')
        return {'status': 'skipped'}

    backup_folder_id = account_details_config.get('backup_folder_id')
    files = Drive.get_files_in_folder(backup_folder_id)
    account_detail_files = [f for f in files if 'account_details' in f.get('name', '')]
    candidate_files = account_detail_files if len(account_detail_files) > 0 else files

    if len(candidate_files) == 0:
        raise Exception('No account details files found in backup folder.')

    most_recent_file = Drive.get_most_recent_file(candidate_files)

    raw_details = Drive.download_file(file_id=most_recent_file['id'], parse=True)
    details = raw_details if isinstance(raw_details, list) else []
    enriched_details = _append_missing_account_details(details)
    file_name = account_details_config['backup_name']

    try:
        existing_file = Drive.get_file_info(parent_id=backup_folder_id, file_name=file_name)
        Drive.delete_file(file_id=existing_file['id'])
    except:
        pass

    json_bytes = json.dumps(enriched_details, default=str).encode('utf-8')
    json_base64 = base64.b64encode(json_bytes).decode('utf-8')
    Drive.upload_file(
        file_name=file_name,
        mime_type='application/json',
        file_data=json_base64,
        parent_folder_id=backup_folder_id
    )

    logger.announcement('Account details backup extracted, enriched, and uploaded.', type='success')

    return {'status': 'success', 'processed_file': most_recent_file.get('name'), 'saved_file': file_name}

# Market data
def extract_bond_snapshot(config=None):
    """
    Extract the bond snapshot.
    
    :return: Response object with bond snapshot or error message
    """
    try:
        ip = requests.get('https://api.ipify.org').content.decode('utf8')
        ibkr_web_api.create_sso_session('agmtech212', ip)
        ibkr_web_api.initialize_brokerage_session()
        time.sleep(2)

        retry_count = 0
        conids = []

        while retry_count < 5:
            watchlist_information = ibkr_web_api.get_watchlist_information('100')

            for watchlist_item in watchlist_information['instruments']:
                if 'assetClass' in watchlist_item.keys() and watchlist_item['assetClass'] == 'BOND':
                    conids.append(str(watchlist_item['conid']))

            if len(conids) > 500:
                break
                
            retry_count += 1
            time.sleep(2)

        first_snapshot = ibkr_web_api.get_market_data_snapshot(','.join(conids[:100]))
        second_snapshot = ibkr_web_api.get_market_data_snapshot(','.join(conids[101:200]))
        third_snapshot = ibkr_web_api.get_market_data_snapshot(','.join(conids[201:300]))
        fourth_snapshot = ibkr_web_api.get_market_data_snapshot(','.join(conids[301:400]))
        fifth_snapshot = ibkr_web_api.get_market_data_snapshot(','.join(conids[401:500]))
        df = pd.DataFrame(first_snapshot + second_snapshot + third_snapshot + fourth_snapshot + fifth_snapshot)
        df.columns = df.columns.str.capitalize()

        df['Financial Instrument'] = df['Symbol'] + ' ' + df['Contract_description_2']
        df['Symbol'] = 'IBCID' + df['Conidex']

        df = df.dropna(subset=['Financial Instrument'])

        from components.tools.public.trade_tickets import extract_bond_details
        for index, row in df.iterrows():
            bond_details = extract_bond_details(row['Financial Instrument'])
            df.at[index, 'Coupon'] = float(bond_details['coupon']) if bond_details['coupon'] != '' else 0.0
            df.at[index, 'Maturity'] = bond_details['maturity']
            df.at[index, 'ISIN'] = bond_details['isin']
        
        # 
        df['Next Option Date'] = ''
        df['Payment Frequency'] = ''
        df['Trading Currency'] = 'USD'
        df['Sector'] = ''

        # Parse the last price robustly: try float, then int, discard zeros/invalids
        def _parse_last_price(value):
            """Return price as float or int; None if it cannot be parsed or equals zero."""
            if value is None:
                return None
            # Extract numeric characters (including decimal point)
            numeric_match = re.findall(r"[\d\.]+", str(value))
            if not numeric_match:
                return None
            numeric_str = numeric_match[0]

            # Attempt float conversion first
            try:
                price_val = float(numeric_str)
                if price_val != 0:
                    return price_val
            except (ValueError, TypeError):
                pass

            # Fallback to int conversion
            try:
                price_val = int(numeric_str)
                if price_val != 0:
                    return price_val
            except (ValueError, TypeError):
                pass

            return None  # Could not parse or value is zero

        df['Last'] = df['Last_price'].apply(_parse_last_price)
        df['Last'] = pd.to_numeric(df['Last'], errors='coerce')
        df = df[df['Last'].notnull()]

        df['Current Yield'] = ((100 * df['Coupon'].astype(float)) / df['Last'].astype(float))

        rename_map = {
            'Company_name': 'Company Name',
            'Bid_size': 'Bid Size',
            'Bid_price': 'Bid',
            'Bid_yield': 'Bid Yield',
            'Ask_size': 'Ask Size',
            'Ask_price': 'Ask',
            'Ask_yield': 'Ask Yield',
            'Issue_date': 'Issue Date',
            'Last_trading_date': 'Last Trading Date',
        }

        df = df.rename(columns=rename_map)
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        df['Timestamp'] = timestamp

        report_config = config or _get_file_config('bonds_snapshot', [market_data_etl])
        if report_config is None:
            raise Exception('Missing report config for bonds_snapshot')
        file_name = report_config['backup_name']
        backup_folder_id = report_config['backup_folder_id']
        Drive.upload_file(file_name=file_name, mime_type='text/csv', file_data=df.to_dict(orient='records'), parent_folder_id=backup_folder_id)
        return df

    except Exception as e:
        logger.error(f'Error extracting bond snapshot: {e}')
        raise Exception(f'Error extracting bond snapshot: {e}')

def extract_stock_snapshot(config=None):
    
    from src.utils.connectors.ibkr_web_api import IBKRWebAPI
    ibkr_web_api = IBKRWebAPI()
    ip = requests.get('https://api.ipify.org').content.decode('utf8')
    ibkr_web_api.create_sso_session('agmtech212', ip)
    ibkr_web_api.initialize_brokerage_session()
    time.sleep(2)

    stock_snapshot_tickers = [
        ('SPY', '756733'),
        ('QQQ', '320227571'),
        ('AAPL', '265598'),
        ('AMZN', '3691937'),
        ('NVDA', '4815747'),
        ('META', '107113386'),
        ('MSFT', '272093'),
        ('GOOGL', '208813720'),
        ('TSLA', '76792991'),
        ('NFLX', '15124833'),
        ('AMD', '4391'),
        ('COIN', '459530964'),
        ('BRK.B', '265598'),  # TODO: verify conid
        ('MA', '38708077'),
        ('DIS', '6459'),
        ('XOM', '13977'),
        ('GLD', '756733'),  # TODO: verify conid
    ]
    conids = ','.join([conid for _, conid in stock_snapshot_tickers])
    
    snapshot = ibkr_web_api.get_market_data_snapshot(conids)
    df = pd.DataFrame(snapshot)

    df.columns = df.columns.str.capitalize()

    df['Financial Instrument'] = df['Symbol']
    df['Symbol'] = 'IBCID' + df['Conidex']
    df['Next Option Date'] = ''
    df['Payment Frequency'] = ''
    df['Trading Currency'] = 'USD'
    df['Sector'] = ''

    def extract_numbers(text):
        return ''.join(filter(str.isdigit, text))

    df['Last'] = df['Last_price'].astype(str).apply(extract_numbers)
    df['Last'] = pd.to_numeric(df['Last'], errors='coerce').fillna(0.0)

    rename_map = {
        'Company_name': 'Company Name',
        'Bid_size': 'Bid Size',
        'Bid_price': 'Bid',
        'Bid_yield': 'Bid Yield',
        'Ask_size': 'Ask Size',
        'Ask_price': 'Ask',
        'Ask_yield': 'Ask Yield',
        'Issue_date': 'Issue Date',
        'Last_trading_date': 'Last Trading Date',
    }

    df = df.rename(columns=rename_map)
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    df['Timestamp'] = timestamp

    market_data_snapshot_config = config or _get_file_config('stocks_snapshot', [market_data_etl])
    if market_data_snapshot_config is None:
        raise Exception('Missing report config for stocks_snapshot')
    file_name = market_data_snapshot_config['backup_name']
    Drive.upload_file(file_name=file_name, mime_type='text/csv', file_data=df.to_dict(orient='records'), parent_folder_id=market_data_snapshot_config['backup_folder_id'])
    return df

def _collect_watchlist_bond_conids(api_client, watchlist_id: str, max_retries: int = 5, sleep_seconds: int = 2, target_size: int = 500) -> list:
    """Collect unique BOND conids from a watchlist with retry polling."""
    unique_conids = []
    seen_conids = set()

    for _ in range(max_retries):
        watchlist_information = api_client.get_watchlist_information(watchlist_id)
        instruments = watchlist_information.get('instruments', [])

        for watchlist_item in instruments:
            if watchlist_item.get('assetClass') != 'BOND':
                continue

            conid = watchlist_item.get('conid')
            if conid is None:
                continue

            conid = str(conid)
            if conid in seen_conids:
                continue

            seen_conids.add(conid)
            unique_conids.append(conid)

        if len(unique_conids) >= target_size:
            break

        time.sleep(sleep_seconds)

    return unique_conids

def _get_market_data_snapshot_in_chunks(api_client, conids: list, chunk_size: int = 75, sleep_seconds: float = 0.25) -> list:
    """Fetch market snapshots in smaller chunks to stay under IBKR conid limits."""
    if chunk_size <= 0:
        raise ValueError('chunk_size must be greater than zero')

    snapshots = []
    total_chunks = (len(conids) + chunk_size - 1) // chunk_size

    for chunk_index, start in enumerate(range(0, len(conids), chunk_size), start=1):
        chunk = conids[start:start + chunk_size]
        if not chunk:
            continue

        logger.info(f'Fetching market snapshot chunk {chunk_index}/{total_chunks} ({len(chunk)} conids)')
        snapshots.extend(api_client.get_market_data_snapshot(','.join(chunk)))

        if sleep_seconds > 0 and chunk_index < total_chunks:
            time.sleep(sleep_seconds)

    return snapshots

def extract_ust_bond_snapshot(config=None):
    
    from src.utils.connectors.ibkr_web_api import IBKRWebAPI
    from src.utils.connectors.drive import GoogleDrive
    drive = GoogleDrive()
    ibkr_web_api = IBKRWebAPI()
    ip = requests.get('https://api.ipify.org').content.decode('utf8')
    ibkr_web_api.create_sso_session('agmtech212', ip)
    ibkr_web_api.initialize_brokerage_session()
    time.sleep(2)

    ust_conids = _collect_watchlist_bond_conids(
        api_client=ibkr_web_api,
        watchlist_id='122',
        max_retries=5,
        sleep_seconds=2,
        target_size=500
    )
    if not ust_conids:
        raise Exception('No UST bond conids found in watchlist 122')

    snapshot = _get_market_data_snapshot_in_chunks(
        api_client=ibkr_web_api,
        conids=ust_conids[:350],
        chunk_size=75,
        sleep_seconds=0.25
    )
    df = pd.DataFrame(snapshot)
    df.columns = df.columns.str.capitalize()

    df['Financial Instrument'] = df['Symbol'] + ' ' + df['Contract_description_2']
    df['Symbol'] = 'IBCID' + df['Conidex']

    from components.tools.public.trade_tickets import extract_bond_details
    for index, row in df.iterrows():
        bond_details = extract_bond_details(row['Financial Instrument'])
        coupon = bond_details.get('coupon')
        try:
            df.at[index, 'Coupon'] = float(coupon) if coupon not in (None, '') else 0.0
        except (TypeError, ValueError):
            df.at[index, 'Coupon'] = 0.0
        df.at[index, 'Maturity'] = bond_details['maturity']
        df.at[index, 'ISIN'] = bond_details['isin']
    
    # 
    df['Next Option Date'] = ''
    df['Payment Frequency'] = ''
    df['Trading Currency'] = 'USD'
    df['Sector'] = ''

    def extract_numbers(text):
        return ''.join(filter(str.isdigit, text))

    df['Last'] = df['Last_price'].astype(str).apply(extract_numbers)
    # Convert extracted strings to numeric, coercing errors to NaN, then replace NaN with 0.0 to avoid ValueError
    df['Last'] = pd.to_numeric(df['Last'], errors='coerce').fillna(0.0)

    df['Current Yield'] = ((1000 * df['Coupon'].astype(float)) / df['Last'].astype(float)) * 100

    rename_map = {
        'Company_name': 'Company Name',
        'Bid_size': 'Bid Size',
        'Bid_price': 'Bid',
        'Bid_yield': 'Bid Yield',
        'Ask_size': 'Ask Size',
        'Ask_price': 'Ask',
        'Ask_yield': 'Ask Yield',
        'Issue_date': 'Issue Date',
        'Last_trading_date': 'Last Trading Date',
    }

    df = df.rename(columns=rename_map)
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    df['Timestamp'] = timestamp

    market_data_snapshot_config = config or _get_file_config('ust_bonds_snapshot', [market_data_etl])
    if market_data_snapshot_config is None:
        raise Exception('Missing report config for ust_bonds_snapshot')

    file_name = market_data_snapshot_config['backup_name']
    Drive.upload_file(
        file_name=file_name,
        mime_type='text/csv',
        file_data=df.to_dict(orient='records'),
        parent_folder_id=market_data_snapshot_config['backup_folder_id']
    )
    return df

def extract_sovereign_bond_snapshot():
    
    from src.utils.connectors.ibkr_web_api import IBKRWebAPI
    from src.utils.connectors.drive import GoogleDrive
    drive = GoogleDrive()
    ibkr_web_api = IBKRWebAPI()
    ip = requests.get('https://api.ipify.org').content.decode('utf8')
    ibkr_web_api.create_sso_session('agmtech212', ip)
    ibkr_web_api.initialize_brokerage_session()
    time.sleep(2)

    retry_count = 0

    ust_conids = []
    while retry_count < 5:
        ust_watchlist_information = ibkr_web_api.get_watchlist_information('179')

        for watchlist_item in ust_watchlist_information['instruments']:
            if 'assetClass' in watchlist_item.keys() and watchlist_item['assetClass'] == 'BOND':
                ust_conids.append(str(watchlist_item['conid']))

        if len(ust_conids) > 500:
            break
            
        retry_count += 1
        time.sleep(2)
    
    snapshot = ibkr_web_api.get_market_data_snapshot(','.join(ust_conids[:350]))
    df = pd.DataFrame(snapshot)
    df.columns = df.columns.str.capitalize()

    df['Financial Instrument'] = df['Symbol'] + ' ' + df['Contract_description_2']
    df['Symbol'] = 'IBCID' + df['Conidex']

    from components.tools.public.trade_tickets import extract_bond_details
    for index, row in df.iterrows():
        bond_details = extract_bond_details(row['Financial Instrument'])
        df.at[index, 'Coupon'] = float(bond_details['coupon']) if bond_details['coupon'] != '' else 0.0
        df.at[index, 'Maturity'] = bond_details['maturity']
        df.at[index, 'ISIN'] = bond_details['isin']
    
    # 
    df['Next Option Date'] = ''
    df['Payment Frequency'] = ''
    df['Trading Currency'] = 'USD'
    df['Sector'] = ''

    def extract_numbers(text):
        return ''.join(filter(str.isdigit, text))

    df['Last'] = df['Last_price'].astype(str).apply(extract_numbers)
    # Convert extracted strings to numeric, coercing errors to NaN, then replace NaN with 0.0 to avoid ValueError
    df['Last'] = pd.to_numeric(df['Last'], errors='coerce').fillna(0.0)

    df['Current Yield'] = ((1000 * df['Coupon'].astype(float)) / df['Last'].astype(float)) * 100

    rename_map = {
        'Company_name': 'Company Name',
        'Bid_size': 'Bid Size',
        'Bid_price': 'Bid',
        'Bid_yield': 'Bid Yield',
        'Ask_size': 'Ask Size',
        'Ask_price': 'Ask',
        'Ask_yield': 'Ask Yield',
        'Issue_date': 'Issue Date',
        'Last_trading_date': 'Last Trading Date',
    }

    df = df.rename(columns=rename_map)
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    df['Timestamp'] = timestamp
    pass

"""
BACKUP
"""
def backup_data(etl_config) -> dict:
    rename_overview = rename_files_in_batch(etl_config)
    sort_overview = sort_batch_files_to_backup_folders(etl_config)
    clear_status = {'name': 'clear_batch_folder', 'status': 'success'}
    try:
        Drive.clear_folder(folder_id=batch_folder_id)
    except Exception as e:
        logger.error(f'Error clearing batch folder: {e}')
        clear_status = {'name': 'clear_batch_folder', 'status': 'failed', 'error': str(e)}

    steps = [rename_overview, sort_overview, clear_status]
    failed_steps = [step for step in steps if step.get('status') != 'success']
    return {
        'status': 'success' if not failed_steps else 'partial',
        'steps': steps
    }

def rename_files_in_batch(etl_config):
    """
    Rename files in the batch folder based on specific naming conventions.
    :return: Response object with updated batch files or error message
    """
    logger.announcement('Renaming files in batch folder.', type='info')
    renamed_files = []
    failed_files = []
    try:
        batch_files = Drive.get_files_in_folder(batch_folder_id)
        
        configs_to_process = _resolve_pipeline_reports(etl_config)

        for f in batch_files:
            for config in configs_to_process:
                if not config.get('backup_folder_id'):
                    continue
                if config['name'] in f['name']:
                    new_name = config['backup_name']
                    try:
                        Drive.rename_file(file_id=f['id'], new_name=new_name)
                        renamed_files.append({'from': f['name'], 'to': new_name, 'status': 'success'})
                    except Exception as e:
                        logger.error(f"Error renaming file {f.get('name')} to {new_name}: {e}")
                        failed_files.append({'from': f.get('name'), 'to': new_name, 'status': 'failed', 'error': str(e)})
    except:
        logger.error(f'Error renaming files in batch.')
        raise Exception(f'Error renaming files in batch.')
    logger.announcement('Files renamed successfully.', type='success')
    return {
        'name': 'rename_batch_files',
        'status': 'success' if not failed_files else 'partial',
        'summary': {
            'renamed': len(renamed_files),
            'failed': len(failed_files)
        },
        'renamed_files': renamed_files,
        'failed_files': failed_files
    }

def sort_batch_files_to_backup_folders(etl_config):
    """
    Sort files from the batch folder into their respective backup folders.
    
    :param batch_files: List of files in the batch folder
    :return: Response object with success message or error
    """
    logger.announcement('Sorting files to backup folders.', type='info')
    moved_files = []
    failed_files = []
    try:
        batch_files = Drive.get_files_in_folder(batch_folder_id)
        
        configs_to_process = _resolve_pipeline_reports(etl_config)

        for f in batch_files:
            for config in configs_to_process:
                if not config.get('backup_folder_id'):
                    continue
                if config['name'] in f['name']:
                    new_parent_id = config['backup_folder_id']
                    try:
                        backup_files = Drive.get_files_in_folder(new_parent_id)
                        if len(backup_files) > 0:
                            for backed_up_file in backup_files:
                                if backed_up_file['createdTime'].split('T')[0] == datetime.now().strftime('%Y-%m-%d'):
                                    logger.warning(f'Deleting backed up file: {backed_up_file}')
                                    Drive.delete_file(backed_up_file['id'])
                        Drive.move_file(f=f, newParentId=new_parent_id)
                        moved_files.append({
                            'file': f.get('name'),
                            'backup_folder_id': new_parent_id,
                            'status': 'success'
                        })
                    except Exception as e:
                        logger.error(f"Error moving file {f.get('name')} to folder {new_parent_id}: {e}")
                        failed_files.append({
                            'file': f.get('name'),
                            'backup_folder_id': new_parent_id,
                            'status': 'failed',
                            'error': str(e)
                        })
    except:
        logger.error(f'Error sorting files to backup folders.')
        raise Exception(f'Error sorting files to backup folders.')
    logger.announcement('Files sorted to backup folders successfully.', type='success')
    return {
        'name': 'sort_batch_files',
        'status': 'success' if not failed_files else 'partial',
        'summary': {
            'moved': len(moved_files),
            'failed': len(failed_files)
        },
        'moved_files': moved_files,
        'failed_files': failed_files
    }

"""
TRANSFORM
"""

def transform_data(etl_config) -> dict:
    logger.announcement('Transforming backups into reports.', type='info')

    # Process files in each backup folder
    logger.announcement('Processing files.', type='info')
    
    configs_to_process = _resolve_pipeline_reports(etl_config)

    processed_reports = []
    for config in configs_to_process:
        if not config.get('backup_folder_id'):
            logger.warning(f"Skipping report '{config.get('name')}' because backup_folder_id is empty.")
            continue
        try:
            report_result = process_report(config)
            processed_reports.append(report_result)
        except Exception as e:
            logger.error(f"Failed transforming report '{config.get('name')}': {e}")
            processed_reports.append({
                'name': config.get('name'),
                'status': 'failed',
                'error': str(e)
            })
    logger.announcement('Files processed.', type='success')

    logger.announcement('Backups successfully transformed into reports.', type='success')
    failed_reports = [report for report in processed_reports if report.get('status') != 'success']
    return {
        'status': 'success' if not failed_reports else 'partial',
        'summary': {
            'total': len(processed_reports),
            'successful': len(processed_reports) - len(failed_reports),
            'failed': len(failed_reports)
        },
        'reports': processed_reports
    }

def process_report(config):
    """
    Process a single report according to its configuration.
    
    :param config: Dictionary containing report configuration
    :return: Response object with success message or error
    """
    try:
        logger.info(f'Processing {config} file.')
        folder_id = config['backup_folder_id']
        output_filename = config['output_filename']
        
        # Get files in the report's backup folder
        files = Drive.get_files_in_folder(folder_id)
        config_name = config['name']
        backup_name = config.get('backup_name', '')

        # Avoid substring collisions (e.g., bonds_snapshot vs ust_bonds_snapshot).
        # Prefer exact backup_name, then strict "<config_name>_" prefix.
        files_for_config = [f for f in files if f.get('name', '') == backup_name]
        if not files_for_config:
            files_for_config = [f for f in files if f.get('name', '').startswith(f'{config_name}_')]
        files = files_for_config or files

        if len(files) == 0:
            logger.error(f'No files found in backup folder.')
            raise Exception('No files found in backup folder.')

        # Get most recent file
        most_recent_file = Drive.get_most_recent_file(files)

        # For very large files that do not require row-level transformations,
        # avoid parsing into DataFrame/records to reduce memory and runtime.
        if config.get('raw_passthrough', False):
            try:
                raw_file = Drive.download_file(file_id=most_recent_file['id'], parse=False)
            except:
                try:
                    raw_file = Drive.export_file(
                        file_id=most_recent_file['id'],
                        mime_type='text/csv',
                        parse=False
                    )
                except:
                    logger.error(f'Error downloading file: {most_recent_file}')
                    raise Exception(f'Error downloading file: {most_recent_file}')

            output_mime_type = 'application/json' if output_filename.lower().endswith('.json') else 'text/csv'
            file_payload = base64.b64encode(raw_file).decode('utf-8')

            try:
                existing_files = [
                    file for file in Drive.get_files_in_folder(resources_folder_id)
                    if file.get('name') == output_filename
                ]
                for existing_file in existing_files:
                    Drive.delete_file(file_id=existing_file['id'])
            except:
                pass

            Drive.upload_file(
                file_name=output_filename,
                mime_type=output_mime_type,
                file_data=file_payload,
                parent_folder_id=resources_folder_id
            )
            return {
                'name': config_name,
                'status': 'success',
                'backup_file': most_recent_file.get('name'),
                'output_file': output_filename
            }

        # Download file and read into dataframe
        try:
            f = Drive.download_file(file_id=most_recent_file['id'], parse=True)
        except:
            try:
                f = Drive.export_file(file_id=most_recent_file['id'], mime_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', parse=True)
            except:
                logger.error(f'Error downloading file: {most_recent_file}')
                raise Exception(f'Error downloading file: {most_recent_file}')
        file_df = pd.DataFrame(f)
        file_df = file_df.fillna('')

        transform_func = config.get('transform_func')
        transformed_content = transform_func(file_df) if transform_func is not None else file_df
        output_mime_type = 'text/csv'

        # Default behavior remains CSV. For .json outputs, store as JSON.
        if output_filename.lower().endswith('.json'):
            output_mime_type = 'application/json'
            if isinstance(transformed_content, pd.DataFrame):
                json_content = transformed_content.to_dict(orient='records')
            else:
                json_content = transformed_content

            try:
                json_bytes = json.dumps(json_content, default=str).encode('utf-8')
            except TypeError as json_error:
                logger.error(f'Error serializing JSON output for {output_filename}: {json_error}')
                raise

            file_payload = base64.b64encode(json_bytes).decode('utf-8')
        else:
            if isinstance(transformed_content, pd.DataFrame):
                csv_content = transformed_content.to_dict(orient='records')
            else:
                csv_content = pd.DataFrame(transformed_content).to_dict(orient='records')
            file_payload = csv_content
        
        # Delete all existing output files with the same name to avoid duplicates.
        try:
            existing_files = [
                file for file in Drive.get_files_in_folder(resources_folder_id)
                if file.get('name') == output_filename
            ]
            for existing_file in existing_files:
                Drive.delete_file(file_id=existing_file['id'])
        except:
            pass

        Drive.upload_file(file_name=output_filename, mime_type=output_mime_type, file_data=file_payload, parent_folder_id=resources_folder_id)
    except:
        logger.error(f'Error processing {config} file.')
        raise Exception(f'Error processing {config} file.')
    logger.announcement(f'{config} successfully processed.', type='success')
    return {
        'name': config_name,
        'status': 'success',
        'backup_file': most_recent_file.get('name'),
        'output_file': output_filename
    }

# Clients
def process_open_positions_template(df):
    """
    Process the open positions template file.
    
    This function performs the following steps:
    1. Upload the full file to the resources folder
    2. Filter the dataframe for BOND asset class and LOT level of detail
    3. Reorder columns according to the specified order
    
    :param df: Input dataframe
    :return: Processed dataframe
    """
    # Upload the full file
    try:
        existing_file = Drive.get_file_info(parent_id=resources_folder_id, file_name='ibkr_open_positions_all.csv')
        Drive.delete_file(file_id=existing_file['id'])
    except:
        pass

    full_dict = df.to_dict(orient='records')
    Drive.upload_file(file_name='ibkr_open_positions_all.csv', mime_type='text/csv', file_data=full_dict, parent_folder_id=resources_folder_id)
    
    # Generate template (extract bonds and details)
    df = df[(df['AssetClass'] == 'BOND') & (df['LevelOfDetail'] == 'LOT')]

    # Extract only the columns that are needed
    file_columns = [
        'ClientAccountID',
        'AccountAlias',
        'Model',
        'CurrencyPrimary',
        'FXRateToBase',
        'AssetClass',
        'Symbol',
        'Description',
        'Conid',
        'SecurityID',
        'SecurityIDType',
        'CUSIP',
        'ISIN',
        'ListingExchange',
        'UnderlyingConid',
        'UnderlyingSymbol',
        'UnderlyingSecurityID',
        'UnderlyingListingExchange',
        'Issuer',
        'Multiplier',
        'Strike',
        'Expiry',
        'Put/Call',
        'PrincipalAdjustFactor',
        'ReportDate',
        'Quantity',
        'MarkPrice',
        'PositionValue',
        'PositionValueInBase',
        'OpenPrice',
        'CostBasisPrice',
        'CostBasisMoney',
        'PercentOfNAV',
        'FifoPnlUnrealized',
        'UnrealizedCapitalGainsPnl',
        'UnrealizedFxPnl',
        'Side',
        'LevelOfDetail',
        'OpenDateTime',
        'HoldingPeriodDateTime',
        'Code',
        'OriginatingOrderID',
        'OriginatingTransactionID',
        'AccruedInterest',
        'VestingDate',
        'SerialNumber',
        'DeliveryType',
        'CommodityType',
        'Fineness',
        'Weight'
    ]
    df = df[file_columns]

    # Create formula columns
    formula_columns = [
        'KEY',
        'securities_bond row',
        'Maturity',
        'Coupon',
        'Sector',
        'Frequency',
        'Open Date',
        'Column5',
        'Column6',
        'Tasa',
        'Column7',
        'Meses en Cartera',
        'Rendimiento Acumulado x Cupón',
        'Current Price - 100',
        'Duraciones',
        'MDURATION',
        'DURATION',
        'Column8',
        'Market Price',
        'Yield',
        'Rate',
        'RTD MATCH CONID',
        'RTD Duration',
        'RTD Bid',
        'RTD Ask',
        'RTD Credit Rating',
        'RTD Credit Rating Level',
        'RTD Ask Value',
        'a',
        'a2',
        'Change in Price + Accrued Interest Received',
        'Year on Portfolio',
        'Yield (Price + Interest)',
        'a3',
        'Duration FX',
        'Coupon * Quantity',
        'Credit Rating Main Class',
        'Investment Grade Amt',
        'Column1',
        'Issuer FX'
    ]
    formula_df = pd.DataFrame(columns=formula_columns)

    # Concatenate the two dataframes horizontally
    concatenated_df = pd.concat([df, formula_df], axis=1)
    concatenated_df = concatenated_df.fillna('')

    rtd = get_bond_report()
    rtd_df = pd.DataFrame(rtd)
    rtd_df['Symbol'] = rtd_df['Symbol'].astype(str).str.strip().str.replace(r'^IBCID', '', regex=True).astype(int)

    final_df = pd.merge(concatenated_df, rtd_df, left_on='Conid', right_on='Symbol', how='right')

    return final_df

# Market data
def process_bonds(df):
    """
    Process the Bonds file.
    
    :param df: Input dataframe
    :return: Processed dataframe
    """
    
    required_columns = ['Symbol',
            'Financial Instrument',
            'Company Name',
            'Bid Size',
            'Bid',
            'Bid Yield',
            'Ask Size',
            'Ask',
            'Ask Yield',
            'Industry',
            'Sector',
            #'Duration %  ',
            'Current Yield',
            'Maturity',
            'Next Option Date',
            'Coupon',
            'Last',
            'Ratings',
            'Payment Frequency',
            'Trading Currency',
            'Issue Date',
            'Last Trading Date',
            #'Face Value',
            #"Moody's",
            #'S&P',
            #'Bond Features',
            #'Time-To-Maturity (TTM)'
            ]

    # Keep transform resilient when upstream snapshots have partial schemas.
    for column in required_columns:
        if column not in df.columns:
            df[column] = ''

    df = df[required_columns]
    
    numeric_columns = ['Bid',
            'Ask',
            'Bid Yield',
            'Ask Yield',
            'Bid Size',
            'Ask Size',
            'Current Yield',
            #'Duration %  ',
            'Coupon',
            'Last',
            #'Face Value',
            #'Time-To-Maturity (TTM)'
            ]

    for column in numeric_columns:
        df[column] = pd.to_numeric(df[column], errors='coerce')

    date_columns = [
        'Next Option Date',
        'Last Trading Date',
        'Maturity',
        'Issue Date'
        ]
    
    for column in date_columns:
        try:
            df[column] = pd.to_datetime(df[column], errors='coerce')
        except:
            df[column] = df[column]

    df['CY'] = df.apply(lambda row: get_current_yield(row['Coupon'], row['Last'], row['Ask'], row['Bid']), axis=1)
    df['Price'] = df.apply(lambda row: get_first_valid_price(row['Ask'], row['Bid'], row['Last']), axis=1)
    df['Frequency'] = df['Payment Frequency'].apply(get_payment_frequency_from_text)
    df['Price Cluster'] = df['Price'].apply(get_bond_price_cluster)
    df['Size Preasure'] = df.apply(lambda row: get_size_preasure(row['Ask Size'], row['Bid Size']), axis=1)
    df['Duration'] = df.apply(lambda row: get_bond_duration(row['Maturity'], 
        row['Coupon']/100, # Convert coupon percentage to decimal
        row['Price'],
        row['Frequency']), 
        axis=1)
    
    # Create new columns for Moody's and S&P ratings
    df['Moodys'], df['SP'] = zip(*df['Ratings'].apply(extract_rating_from_text))
    df['Moodys'] = df['Moodys'].apply(clean_rating_text)
    df['SP'] = df['SP'].apply(clean_rating_text)

    # Extract first chars before space from Financial Instrument column
    df['Issuer'] = df['Financial Instrument'].str.split().str[0]

    #Calculate years to maturity
    today = pd.Timestamp.today()
    df['Years to Maturity'] = df.apply(lambda row: get_years_to_date(today, row['Maturity']), axis=1)
    df['YTM'] = df.apply(lambda row: get_bond_ytm(row['Price'], row['Coupon'], row['Years to Maturity']), axis=1)

    def get_rating_level(rating):
        """
        Get the level for a single rating from the ratings dictionary.
        Returns None if rating is not found.
        """
        if pd.isna(rating) or rating not in ratings:
            return None
        return ratings[rating]["Level"]

    # Get levels for both SP and Moodys ratings
    df['SP_Level'] = df['SP'].apply(get_rating_level)
    df['Moodys_Level'] = df['Moodys'].apply(get_rating_level)

    # Coerce to numeric before row-wise max to avoid float/string comparison errors.
    level_columns = ['SP_Level', 'Moodys_Level']
    df[level_columns] = df[level_columns].apply(pd.to_numeric, errors='coerce')

    # Get the lowest (highest number) level between SP and Moodys
    df['Rating Level'] = df[level_columns].max(axis=1)

    # Drop the temporary columns
    df = df.drop(['SP_Level', 'Moodys_Level'], axis=1)

    # Create S&P Equivalent column by mapping Rating Level to S&P rating
    def get_sp_equivalent(level):
        """
        Get the S&P equivalent rating for a given level from the ratings dictionary.
        Returns None if level is not found.
        """
        if pd.isna(level):
            return None
        
        # Find the first rating entry that matches the level
        for rating, info in ratings.items():
            if info["Level"] == level:# and info["Source"] == "S&P":
                return rating
        return None

    df['S&P Equivalent'] = df['Rating Level'].apply(get_sp_equivalent)

    return df

"""
CONFIGS
"""
market_data_report_configs = [
    {
        'name': 'bonds_snapshot',
        'pipeline': 'market_data',
        'backup_folder_id': '1luTnQ1qRDNWLrqjMan-kF_eMgH16R-J9',
        'flex': False,
        'backup_name': 'bonds_snapshot' + '_' + today_date + '.csv',
        'extract_func': extract_bond_snapshot,
        'transform_func': process_bonds,
        'output_filename': 'ibkr_bonds_snapshot.csv'
    },
    {
        'name': 'ust_bonds_snapshot',
        'pipeline': 'market_data',
        'backup_folder_id': '1luTnQ1qRDNWLrqjMan-kF_eMgH16R-J9',
        'flex': False,
        'backup_name': 'ust_bonds_snapshot' + '_' + today_date + '.csv',
        'extract_func': extract_ust_bond_snapshot,
        'transform_func': process_bonds,
        'output_filename': 'ibkr_ust_bonds_snapshot.csv'
    },
    {
        'name': 'stocks_snapshot',
        'pipeline': 'market_data',
        'backup_folder_id': '1eo9yhD76i2oDf2UhE-GgVRyMsisf6iZO',
        'flex': False,
        'backup_name': 'stock' + '_' + today_date + '.csv',
        'extract_func': extract_stock_snapshot,
        'transform_func': None,
        'output_filename': 'ibkr_stocks_snapshot.csv'
    }
]

clients_report_configs = [
    {
        'name': 'account_details',
        'pipeline': 'clients',
        'backup_folder_id': '1YCOsFGAb3fZvKbFDBGK6wpjT1S-NIK98',
        'flex': False,
        'backup_name': 'account_details' + '_' + yesterday_date + '.json',
        'extract_func': extract_account_details_backup,
        'transform_func': None,
        'output_filename': 'ibkr_account_details.json'
    },
    {
        'name': 'tasks_for_subaccounts',
        'pipeline': 'clients',
        'backup_folder_id': '1u0IUkD7-lBUy9uhgHD-5xzw3l8OePJdE',
        'flex': False,
        'backup_name': 'tasks_for_subaccounts' + ' ' + today_date + ' ' + 'agmtech212.csv',
        'extract_func': None,
        'transform_func': None,
        'output_filename': 'ibkr_tasks_for_subaccounts.csv',
    },
    {
        'name': 'ContactListSummary',
        'pipeline': 'clients',
        'backup_folder_id': '11rmflCuYs7seB2z1xBGo1n51dGB15L6-',
        'flex': False,
        'backup_name': 'ContactListSummary' + ' ' + today_date + ' ' + 'agmtech212.csv',
        'extract_func': None,
        'transform_func': None,
        'output_filename': 'ibkr_contact_list_summary.csv',
    },
    {
        'name': 'clients',
        'pipeline': 'clients',
        'backup_folder_id': '1FNcbWNptK-A5IhmLws-R2Htl85OSFrIn',
        'flex': False,
        'backup_name': 'clients' + ' ' + today_date +  ' ' + 'agmtech212.xls',
        'extract_func': None,
        'transform_func': None,
        'output_filename': 'ibkr_clients.csv'
    },
    {
        'name': '742588',
        'pipeline': 'clients',
        'backup_folder_id': '1JL4__mr1XgOtnesYihHo-netWKMIGMet',
        'flex': True,
        'backup_name': '742588' + '_' + yesterday_date + '.csv',
        'extract_func': extract_flex_query,
        'transform_func': process_open_positions_template,
        'output_filename': 'ibkr_open_positions_template.csv',
    },
    {
        'name': '734782',
        'pipeline': 'clients',
        'backup_folder_id': '1WgYA-Q9mnPYrbbLfYLuJZwUIWBYjiD4c',
        'flex': True,
        'backup_name': '734782' + '_' + yesterday_date + '.csv',
        'extract_func': extract_flex_query,
        'transform_func': None,
        'output_filename': 'ibkr_nav_in_base.csv'
    },
    {
        'name': '732383',
        'pipeline': 'clients',
        'backup_folder_id': '1OnSEo8B2VUF5u-VkhtzZVIzx6ABe_YB7',
        'flex': True,
        'backup_name': '732383' + '_' + first_date + '_' + yesterday_date + '.csv',
        'extract_func': extract_flex_query,
        'transform_func': None,
        'output_filename': 'ibkr_client_fees.csv'
    },
    {
        'name': '794867',
        'pipeline': 'clients',
        'backup_folder_id': '1ZCJfH2hxvMLuP470HMa-D33_R_l-Lhtx',
        'flex': True,
        'backup_name': '794867' + '_' + first_date + '_' + yesterday_date + '.csv',
        'extract_func': extract_flex_query,
        'transform_func': None,
        'output_filename': 'ibkr_deposits_withdrawals.csv'
    },
    {
        'name': 'ofac_sdn_list',
        'pipeline': 'clients',
        'backup_folder_id': '13W9sXMbFvWtXPsEy6FiZJrQDHV3WYDD6',
        'flex': False,
        'backup_name': 'ofac_sdn_list' + '_' + today_date + '.csv',
        'extract_func': extract_ofac_sdn_list,
        'transform_func': None,
        'output_filename': 'ofac_sdn_list.csv'
    },
    {
        'name': 'uk_sanctions_list',
        'pipeline': 'clients',
        'backup_folder_id': '1-57AG_nFE2elzOygdc7PGqdB4Y9k_7h6',
        'flex': False,
        'backup_name': 'uk_sanctions_list' + '_' + today_date + '.csv',
        'extract_func': extract_uk_sanctions_list,
        'transform_func': None,
        'output_filename': 'uk_sanctions_list.csv',
        'raw_passthrough': True
    },
    {
        'name': 'un_sanctions_list',
        'pipeline': 'clients',
        'backup_folder_id': '1AwTRSLSi0D3kzyhFx9Be53ugvo7uJgpd',
        'flex': False,
        'backup_name': 'un_sanctions_list' + '_' + today_date + '.csv',
        'extract_func': extract_un_sanctions_list,
        'transform_func': None,
        'output_filename': 'un_sanctions_list.csv',
        'raw_passthrough': True
    }
]

clients_etl = {
    'name': 'clients',
    'files': clients_report_configs,
}

market_data_etl = {
    'name': 'market_data',
    'files': market_data_report_configs,
}

ETL_CONFIGS = [clients_etl, market_data_etl]

"""
HELPER FUNCTIONS
"""

def _resolve_pipeline_reports(etl_config):
    if etl_config is None:
        return []
    return etl_config.get('files', [])

def _get_etl_config_by_name(pipeline_name, etl_configs=None):
    if etl_configs is None:
        etl_configs = ETL_CONFIGS

    for etl_config in etl_configs:
        if etl_config.get('name') == pipeline_name:
            return etl_config

    return None

def _get_file_config(file_name, etl_configs=None):
    if etl_configs is None:
        etl_configs = ETL_CONFIGS

    for etl_config in etl_configs:
        for file_config in etl_config.get('files', []):
            if file_config.get('name') == file_name:
                return file_config

    return None

def _run_steps(steps):
    stage_overview = {}
    for step in steps:
        step_name = step['name']
        try:
            stage_overview[step_name] = step['fn']()
        except Exception as e:
            logger.error(f'Error in {step_name} stage: {e}')
            stage_overview[step_name] = {'status': 'failed', 'error': str(e)}

    failed_stages = [name for name, value in stage_overview.items() if value.get('status') != 'success']
    overall_status = 'success' if not failed_stages else 'partial'
    return {
        'status': overall_status,
        'overview': stage_overview
    }

# Process bonds helper functions

def get_bond_price_cluster(price):
    """
    Returns the price cluster label for a given bond price.
    
    Args:
        price (float): Bond price
        
    Returns:
        str: Price cluster label ('Deep Discount', 'Discount', 'Par', 'Premium', or 'High Premium')
    """
    # Define same bins and labels as cluster_bonds_by_price
    bins = [0, 70, 95, 105, 120, float('inf')]
    labels = ['Deep Discount', 'Discount', 'Par', 'Premium', 'High Premium']
    
    # Find which bin the price falls into
    for i in range(len(bins)-1):
        if bins[i] <= price < bins[i+1]:
            return labels[i]
            
    return None  # Return None if price doesn't fall in any bin

def get_first_valid_price(ask, bid, last):
    """
    Returns the first valid price from bid, ask, or last price in that order.
    Returns None if no valid price is found.
    """
    
    if pd.notnull(ask) and ask != 0:
        return ask
    elif pd.notnull(bid) and bid != 0:
        return bid
    elif pd.notnull(last) and last != 0:
        return last
    else:
        return None

def get_size_preasure(BID_size, ASK_size):

    try:
        size_preasure = BID_size - ASK_size
        return size_preasure
    except Exception as e:
        return None

def get_current_yield(coupon, last, ask, bid):
    """
    Calculate current yield using first available price (Last, Ask, or Bid)
    
    Args:
        coupon (float): Bond coupon rate
        last (float): Last traded price
        ask (float): Ask price
        bid (float): Bid price
        
    Returns:
        float: Current yield or None if no valid price available
    """

    if coupon is None or coupon == 0 or not isinstance(coupon, (int, float)):
        return None
    else:
        if pd.notnull(last) and last != 0:
            return coupon / last
        elif pd.notnull(ask) and ask != 0:
            return coupon / ask
        elif pd.notnull(bid) and bid != 0:
            return coupon / bid
        else:
            return None

def get_bond_duration(maturity_date, coupon_rate, price, frequency):
    
    """
    Calculate the Macaulay Duration for a bond
    
    Args:
        maturity_date: The maturity date of the bond (datetime.date)
        coupon_rate: Annual coupon rate as decimal (e.g. 0.05 for 5%)
        price: Clean price of the bond as percentage of par (e.g. 100 for par)
        frequency: Number of coupon payments per year (default=2 for semi-annual)
        
    Returns:
        duration: Macaulay Duration in years
    """

    # Default to semi-annual frequency if not provided because it's the most common frequency
    if frequency is None:
        frequency = 2


    try:
        # Convert maturity_date to datetime.date if it's datetime
        if isinstance(maturity_date, datetime.datetime):
            maturity_date = maturity_date.date()

        today = datetime.date.today()
        
        # Time to maturity in years
        t = (maturity_date - today).days / 360.0

        if t <= 0:
            return 0

        # Convert annual rates to per-period rates
        period_coupon = coupon_rate / frequency

        periods = max(1, int(t * frequency))  # Ensure at least 1 period
        r = ( ( coupon_rate * 100 ) / price) / frequency

        # Calculate present value of each cash flow
        pv_factors = [(1 + r) ** (-i) for i in range(1, periods + 1)]
        cash_flows = [period_coupon * 100] * periods
        cash_flows[-1] += 100  # Add principal repayment at maturity
        
        # Calculate weighted present values
        weighted_pvs = [cf * pvf * (i/frequency) for i, (cf, pvf) in enumerate(zip(cash_flows, pv_factors), 1)]
        
        # Macaulay Duration formula
        duration = sum(weighted_pvs) / price

    except Exception as e:
        return 0

    return duration

def get_payment_frequency_from_text(frequency_text):
    """
    Convert payment frequency text to number of payments per year.
    
    Args:
        frequency_text (str): Payment frequency text (e.g., 'Semi-Annual', 'Quarterly', 'Annual')
        
    Returns:
        int: Number of payments per year (e.g., 2 for Semi-Annual, 4 for Quarterly, 1 for Annual)
    """
    if pd.isna(frequency_text):
        return None
        
    frequency_text = str(frequency_text).lower().strip()
    
    # Common frequency mappings
    frequency_map = {
        'annual': 1,
        'semi-annual': 2,
        'semi annual': 2,
        'quarterly': 4,
        'monthly': 12,
        'zero': 0,
        'zero coupon': 0
    }
    
    # Try exact match first
    if frequency_text in frequency_map:
        return frequency_map[frequency_text]
        
    # Try partial matches
    for key, value in frequency_map.items():
        if key in frequency_text:
            return value
            
    # Default to semi-annual if no match found (most common for bonds)
    return 2

def get_bond_ytm(price, coupon, years_to_maturity):
    try:
        # Use the first available price (Last, Ask, or Bid)
        price = price if pd.notnull(price) else None
                
        if price is None or years_to_maturity == 0:
            return None
            
        numerator = coupon + ((100 - price) / years_to_maturity)
        denominator = (100 + price) / 2
        
        return numerator / denominator
    except:
        return None

def get_years_to_date(start_date, end_date):
    """
    Calculate years to maturity from today to a given maturity date.
    
    Args:
        maturity_date (datetime): The maturity date of the bond
        
    Returns:
        float: Number of years to maturity, or None if invalid input
    """
    try:
        if pd.isna(end_date):
            return None
            
        years = (end_date - start_date).total_seconds() / (365.25 * 24 * 60 * 60)
        return years if years > 0 else None
        
    except:
        return None

def extract_rating_from_text(rating_text):
    """
    Extract rating information from text in formats like:
    - 'CAA3/CCC+ (MOODY/SP)' for dual ratings
    - 'CAA3 (MOODY)' for single rating
    Returns a tuple of (moody_rating, sp_rating)
    """
    rating_text = str(rating_text)
    
    # Find position of '('
    open_paren_pos = rating_text.find('(')
    if open_paren_pos == -1:
        return None, None
        
    # Get the rating and agency parts
    rating_part = rating_text[:open_paren_pos].strip()
    agency_part = rating_text[open_paren_pos+1:-1].strip()
    
    # Initialize ratings
    moody_rating = None
    sp_rating = None
    
    # Check if it's a dual rating format
    if '/' in rating_part and '/' in agency_part:
        ratings = rating_part.split('/')
        agencies = agency_part.split('/')
        
        if len(ratings) == 2 and len(agencies) == 2:
            for rating, agency in zip(ratings, agencies):
                if 'MOODY' in agency.upper():
                    moody_rating = rating.strip()
                elif 'SP' in agency.upper():
                    sp_rating = rating.strip()
    else:
        # Single rating format
        if 'MOODY' in agency_part.upper():
            moody_rating = rating_part.strip()
        elif 'SP' in agency_part.upper():
            sp_rating = rating_part.strip()
            
    return moody_rating, sp_rating

def clean_rating_text(text):
    # Handle None or non-string inputs
    if text is None or not isinstance(text, str):
        return None
        
    # Create new text keeping only characters from chars list
    chars = ['A','a','B','b','C','1','2','3','/','-','+']

    new_text = ''.join(c for c in text if c in chars)
    new_text = new_text.strip()
    return new_text
