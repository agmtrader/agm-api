"""Local statement acceptance environment; no production identities or DB writes.

Run from agm-api: PYTHONPATH=. python tests/fixtures/statement_acceptance_server.py
Default: replay the observed IBKR business response and a synthetic monthly PDF.
Optional live broker reads: STATEMENT_LIVE_READ=1 STATEMENT_ACCOUNT=... .
Only statement generation is sent to IBKR; supporting page reads use seeded data.
"""
import base64
import json
import os
import subprocess
from io import BytesIO

from src.utils.managers import secret_manager

LIVE = os.environ.get('STATEMENT_LIVE_READ') == '1'
ACCOUNT = os.environ.get('STATEMENT_ACCOUNT', 'U-SANITIZED')
MASTER = os.environ.get('STATEMENT_MASTER', 'F10740574')
ACCOUNT_ID = '00000000-0000-4000-8000-000000000001'
ADVISOR_ID = '00000000-0000-4000-8000-000000000002'
CONTACT_ID = '00000000-0000-4000-8000-000000000003'
UNAVAILABLE = ('Daily statements are not produced for account(s) awaiting initial funding. '
               'Monthly statements are provided for all accounts regardless of funding state.')

if LIVE:
    secret = subprocess.run(['gcloud', 'secrets', 'versions', 'access', '1',
        '--secret=IBKR_ACCOUNT_MANAGEMENT_PRIVATE_KEY', '--account=aa@agmtechnology.com',
        '--project=agm-datalake'], capture_output=True, text=True, check=True).stdout
    try:
        secret = json.loads(secret)
    except json.JSONDecodeError:
        pass
    secret_manager._cache_secret('IBKR_ACCOUNT_MANAGEMENT_PRIVATE_KEY', secret)
secret_manager._cache_secret('JWT_SECRET_KEY', 'local-statement-fixture-signing-key-at-least-32-bytes')

from src.components.clients.users import hash_password
from src.utils.connectors import supabase

password_hash = hash_password('local-statement-test')
USERS = [dict(id=role, email=f'{role}@agmtechnology.com', scopes='all' if role == 'operator' else 'advisor',
              password_hash=password_hash) for role in ('operator', 'advisor')]
ROW = dict(id=ACCOUNT_ID, ibkr_account_number=ACCOUNT, master_account=MASTER,
           advisor_code=7, created='20260901000000', application_json=None)
ADVISOR = dict(id=ADVISOR_ID, code=7, contact_id=CONTACT_ID)
CONTACTS = [dict(id=CONTACT_ID, email='advisor@agmtechnology.com', name='Statement Test Advisor')]


class ReadOnlyFixtureStore:
    def __init__(self):
        from sqlalchemy import MetaData, Table, Column
        from sqlalchemy.dialects.postgresql import JSONB
        self.metadata = MetaData()
        Table('account', self.metadata, Column('application_json', JSONB))

    def read(self, table, query=None, **kwargs):
        rows = {'user': USERS, 'account': [ROW], 'advisor': [ADVISOR], 'contact': CONTACTS}.get(table, [])
        return [dict(row) for row in rows if all(str(row.get(k)) == str(v) for k, v in (query or {}).items())]

    def __getattr__(self, name):
        raise AssertionError(f'Unexpected fixture storage operation: {name}')


supabase.db.set_target(ReadOnlyFixtureStore())
supabase.initialize_database = lambda: supabase.db

# Prevent the production app's optional docs generator from modifying source files.
from src.utils.managers import docs_manager
docs_manager.generate_docs = lambda *args, **kwargs: None
from run import app
from src.components.clients import accounts, advisors
from src.components.tools.public import reporting

@app.before_request
def block_unrelated_writes():
    from flask import request
    if request.method != 'GET' and request.path not in {
        '/users/login', '/accounts/ibkr/statements', '/advisors/me/account_statement',
    }:
        return {'error': 'Only login and read-only statement generation are allowed in this fixture.'}, 405

reporting.get_clients_report = lambda *a, **k: [{'Account ID': ACCOUNT, 'Title': 'Statement acceptance account', 'Status': 'Open'}]
reporting.get_nav_report = lambda *a, **k: []
reporting.get_open_positions_report = lambda *a, **k: []
advisors.get_clients_report = reporting.get_clients_report
advisors.get_nav_report = reporting.get_nav_report
advisors.get_open_positions_report = reporting.get_open_positions_report
accounts.read_account_details = lambda *a, **k: {
    'account': {'accountId': ACCOUNT, 'accountTitle': 'Statement acceptance account', 'masterAccountId': MASTER,
                'applicantType': 'Individual', 'clearingStatus': 'O', 'dateOpened': '20260901'},
    'associatedPersons': [],
}
from src.app.tools.public import reporting as reporting_routes
from src.app.clients import accounts as account_routes
reporting_routes.get_clients_report = reporting.get_clients_report
reporting_routes.get_nav_report = reporting.get_nav_report
reporting_routes.get_open_positions_report = reporting.get_open_positions_report
account_routes.read_account_details = accounts.read_account_details
account_routes.get_financial_ranges = lambda *a, **k: {'jsonData': []}

from src.utils.connectors import ibkr_web_api
if not LIVE:
    # The actual connector still constructs/signs the statement request and parses
    # the response. Only the external HTTP boundary and signing credentials differ.
    from requests import Response
    from reportlab.pdfgen import canvas

    secret_manager._cache_secret('IBKR_ACCOUNT_MANAGEMENT_PRIVATE_KEY', 'synthetic-private-key')
    def post(url, *, data, **kwargs):
        assert url == 'https://api.ibkr.com/gw/api/v1/statements'
        body = json.loads(data)
        result = Response(); result.status_code = 200
        if len(body['startDate']) == 6:
            pdf = BytesIO(); document = canvas.Canvas(pdf)
            document.drawString(72, 750, 'Statement acceptance fixture')
            document.drawString(72, 720, f"Account: {body['accountId']}")
            document.drawString(72, 690, f"Period: {body['startDate']} to {body['endDate']}")
            document.drawString(72, 660, f"Language: {body['language']}")
            document.save()
            result.headers['Content-Type'] = 'application/json'
            result._content = json.dumps({'data': {'dataType': 'String', 'mimeType': 'application/pdf',
                'encoding': 'base64', 'value': base64.b64encode(pdf.getvalue()).decode()}}).encode()
        else:
            result.headers['Content-Type'] = 'text/plain;charset=UTF-8'
            result._content = json.dumps({'type': '/simple', 'title': 'OK', 'status': 200,
                'detail': UNAVAILABLE, 'instance': '/statements'}, separators=(',', ':')).encode()
        return result
    ibkr_web_api.requests.post = post
    ibkr_web_api.IBKRWebAPI.get_bearer_token = lambda self: 'synthetic-token'
    ibkr_web_api.IBKRWebAPI.sign_request = lambda self, body: json.dumps(body)

if __name__ == '__main__':
    # Loopback only; seeded credentials are valid only in this isolated process.
    app.run(host='127.0.0.1', port=5000, use_reloader=False, threaded=True)
