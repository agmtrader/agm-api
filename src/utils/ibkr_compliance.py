"""Validate the manually exported Portal inventory without inferring completion."""
import csv
import hashlib
import io
import re
from datetime import datetime, timezone


COLUMNS = {
    'ACCT_ID': 'account_number', 'ACCOUNT_TITLE': 'account_title',
    'FORM_NUMBER': 'form_number', 'FORM_NAME': 'form_name',
    'TASK_NUMBER': 'task_number', 'DATE_ASSIGNED': 'date_assigned',
    'TASK_AGE': 'task_age', 'TASK_STATUS': 'task_status',
    'ACCOUNT_RESTRICTED_STATUS': 'account_restricted_status',
    'ACCOUNT_RESTRICTED_DATE': 'account_restricted_date',
}


def parse_compliance_csv(raw):
    reader = csv.DictReader(io.StringIO(raw.decode('utf-8-sig')), strict=True)
    if not reader.fieldnames or len(set(reader.fieldnames)) != len(reader.fieldnames) or set(reader.fieldnames) != set(COLUMNS):
        raise ValueError('Compliance CSV must contain the original IBKR export columns')
    rows, identities = [], set()
    for line, original in enumerate(reader, 2):
        if None in original or any(value is None for value in original.values()):
            raise ValueError(f'Incomplete compliance CSV row {line}')
        row = {target: original[column].strip() for column, target in COLUMNS.items()}
        if not re.fullmatch(r'[A-Z]\d+', row['account_number']):
            raise ValueError(f'Invalid compliance account at row {line}')
        for field in ('form_number', 'task_number', 'task_age'):
            if not re.fullmatch(r'\d+', row[field]) or (field != 'task_age' and int(row[field]) <= 0):
                raise ValueError(f'Invalid compliance {field} at row {line}')
        # Instance IDs stay strings; never round them through floating point.
        row['form_number'] = int(row['form_number'])
        row['task_age'] = int(row['task_age'])
        row['task_number'] = str(int(row['task_number']))
        if not row['form_name'] or not row['task_status'] or row['account_restricted_status'] not in ('RESTRICTED', 'UNRESTRICTED'):
            raise ValueError(f'Invalid compliance status or name at row {line}')
        datetime.strptime(row['date_assigned'], '%Y-%m-%d %H:%M:%S')
        if row['account_restricted_date']:
            datetime.strptime(row['account_restricted_date'], '%Y-%m-%d')
        identity = (row['account_number'], row['task_number'])
        if identity in identities:
            raise ValueError(f'Duplicate compliance task instance at row {line}')
        identities.add(identity)
        rows.append(row)
    return rows


def build_compliance_report(raw, source):
    return {
        'schema_version': 1,
        'source': {
            'filename': 'compliance_tasks.csv',
            'backup_file': source['name'],
            'file_id': source['id'],
            'uploaded_at': (source.get('appProperties') or {}).get('manual_source_modified_at'),
            # The export has no capture timestamp. Upload/import times are not export times.
            'exported_at': None,
            'sha256': hashlib.sha256(raw).hexdigest(),
        },
        'published_at': datetime.now(timezone.utc).isoformat(),
        'rows': parse_compliance_csv(raw),
    }


def compliance_report_view(report, account_id=None, now=None):
    if not isinstance(report, dict) or report.get('schema_version') != 1 or not isinstance(report.get('rows'), list) or not isinstance(report.get('source'), dict):
        raise ValueError('Invalid compliance report; republish through Clients ETL')
    rows = report['rows']
    if any(not isinstance(row, dict) or not all(field in row for field in COLUMNS.values()) for row in rows):
        raise ValueError('Invalid compliance report rows')
    uploaded_at = report['source'].get('uploaded_at')
    age = None
    if uploaded_at:
        uploaded = datetime.fromisoformat(uploaded_at.replace('Z', '+00:00'))
        age = ((now or datetime.now(timezone.utc)) - uploaded).total_seconds()
    return {
        **report,
        'account_id': account_id,
        'rows': [row for row in rows if account_id is None or row['account_number'] == account_id],
        'freshness': {'uploaded_at': uploaded_at, 'exported_at': None,
                      'stale': age is None or age < 0 or age > 86400},
    }
