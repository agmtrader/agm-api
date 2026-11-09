"""Read a verified account snapshot for an investment proposal (no orders)."""
import ipaddress
import math
import re
from datetime import datetime, timezone

import requests

from src.utils.connectors.ibkr_web_api import IBKRWebAPI
from src.utils.connectors.supabase import db
from src.utils.exception import ServiceError


def _number(value, label):
    try:
        value = float(value)
    except (TypeError, ValueError):
        raise ServiceError(f'IBKR did not provide a valid {label}.', 502, code='portfolio_incomplete') from None
    if not math.isfinite(value):
        raise ServiceError(f'IBKR did not provide a valid {label}.', 502, code='portfolio_incomplete')
    return value


class PortfolioReader:
    """Use the outer SSO session only; never acquire a competing trading session."""

    def __init__(self, master):
        self.signer = IBKRWebAPI()
        if master not in self.signer.MASTER_ACCOUNT_CREDENTIALS:
            raise ServiceError('The account has an unsupported IBKR master.', 422, code='portfolio_master_invalid')
        self.master = master
        self.session_token = None

    def request(self, method, path, *, token=None, payload=None, signed=False, form=False, params=None):
        headers = {'Content-Type': 'application/json'}
        if token or self.session_token:
            headers['Authorization'] = 'Bearer ' + (token or self.session_token)
        kwargs = {'headers': headers, 'timeout': (10, 60), 'allow_redirects': False, 'params': params}
        if signed:
            headers['Content-Type'] = 'application/jwt'
            kwargs['data'] = self.signer.sign_request(payload)
        elif form:
            headers['Content-Type'] = 'application/x-www-form-urlencoded'
            kwargs['data'] = payload
        elif payload is not None:
            kwargs['json'] = payload
        try:
            response = requests.request(method, 'https://api.ibkr.com' + path, **kwargs)
        except requests.RequestException:
            raise ServiceError('IBKR could not be reached. Try again later.', 502, code='portfolio_provider_unavailable') from None
        if response.status_code != 200:
            raise ServiceError(f'IBKR portfolio request failed (HTTP {response.status_code}).', 502, code='portfolio_provider_error')
        try:
            data = response.json()
        except ValueError:
            raise ServiceError('IBKR returned an invalid response.', 502, code='portfolio_provider_error') from None
        if isinstance(data, dict) and (data.get('error') or data.get('rc') not in (None, 0)):
            raise ServiceError('IBKR could not complete the portfolio request.', 502, code='portfolio_provider_error')
        return data

    def oauth(self, master):
        self.signer._apply_credentials(master)
        result = self.request('POST', '/oauth2/api/v1/token', form=True, payload={
            'grant_type': 'client_credentials',
            'client_assertion_type': 'urn:ietf:params:oauth:client-assertion-type:jwt-bearer',
            'client_assertion': self.signer.generate_client_assertion(),
            'scope': 'accounts.read sso-sessions.write',
        })
        token = result.get('access_token') if isinstance(result, dict) else None
        if not isinstance(token, str) or not token:
            raise ServiceError('IBKR authentication failed.', 502, code='portfolio_provider_error')
        return token

    def connect(self):
        management = self.oauth(self.master)
        sso = self.oauth('I6413690') if self.master != 'I6413690' else management
        try:
            response = requests.get('https://api.ipify.org', timeout=(10, 10), allow_redirects=False)
            response.raise_for_status()
            ip = str(ipaddress.ip_address(response.text.strip()))
        except (requests.RequestException, ValueError):
            raise ServiceError('Could not determine the IBKR session IP.', 502, code='portfolio_provider_error') from None
        result = self.request('POST', '/gw/api/v1/sso-sessions', token=sso, signed=True,
                              payload={'credential': 'aguiagm2024', 'ip': ip})
        self.session_token = result.get('access_token') if isinstance(result, dict) else None
        if not isinstance(self.session_token, str) or not self.session_token:
            raise ServiceError('IBKR session creation failed.', 502, code='portfolio_provider_error')
        return management

    def snapshot(self, account):
        management = self.connect()
        details = self.request('GET', f'/gw/api/v1/accounts/{account}/details', token=management)
        details = details.get('account', {}) if isinstance(details, dict) else {}
        if details.get('accountId') != account or details.get('masterAccountId') != self.master:
            raise ServiceError('IBKR account and master could not be verified.', 422, code='portfolio_account_mismatch')
        currency = details.get('baseCurrency')
        if currency != 'USD':
            raise ServiceError('Only USD-base accounts are supported for this scenario.', 422, code='portfolio_currency_unsupported')
        self.request('GET', '/v1/api/portfolio/accounts')
        ledger = self.request('GET', f'/v1/api/portfolio/{account}/ledger')
        cash_rows = {key: value for key, value in ledger.items() if key != 'BASE'} if isinstance(ledger, dict) else {}
        if set(cash_rows) != {'USD'} or cash_rows['USD'].get('acctcode') != account:
            raise ServiceError('The account has missing or multiple currency ledgers.', 422, code='portfolio_currency_unsupported')
        cash = _number(cash_rows['USD'].get('cashbalance'), 'cash balance')
        nav = _number(cash_rows['USD'].get('netliquidationvalue'), 'net liquidation value')
        positions, keys = [], set()
        for page in range(1000):
            rows = self.request('GET', f'/v1/api/portfolio/{account}/positions/{page}')
            if not isinstance(rows, list):
                raise ServiceError('IBKR positions were incomplete.', 502, code='portfolio_incomplete')
            for row in rows:
                if not isinstance(row, dict) or row.get('acctId') != account or row.get('currency') != 'USD':
                    raise ServiceError('IBKR positions contain an unexpected account or currency.', 422, code='portfolio_incomplete')
                key = (row.get('conid'), row.get('model'))
                if not isinstance(key[0], int) or key in keys:
                    raise ServiceError('IBKR positions contain missing or repeated identifiers.', 502, code='portfolio_incomplete')
                keys.add(key)
                positions.append(row)
            if len(rows) < 100:
                break
        else:
            raise ServiceError('IBKR positions exceed the pagination limit.', 502, code='portfolio_incomplete')
        definitions = {}
        conids = [str(row['conid']) for row in positions]
        for offset in range(0, len(conids), 200):
            result = self.request('GET', '/v1/api/trsrv/secdef', params={'conids': ','.join(conids[offset:offset + 200])})
            rows = result.get('secdef') if isinstance(result, dict) else None
            if not isinstance(rows, list):
                raise ServiceError('IBKR security definitions were incomplete.', 502, code='portfolio_incomplete')
            for definition in rows:
                if isinstance(definition, dict) and definition.get('conid') in definitions:
                    raise ServiceError('IBKR returned duplicate security definitions.', 502, code='portfolio_incomplete')
                if isinstance(definition, dict):
                    definitions[definition.get('conid')] = definition
        result = []
        for row in positions:
            definition = definitions.get(row['conid'], {})
            bond = definition.get('bond') or {}
            quantity = _number(row.get('position'), 'position quantity')
            market_value = _number(row.get('mktValue'), 'position market value')
            principal = bond.get('principalValue')
            face = quantity * _number(principal, 'bond principal') if row.get('assetClass') == 'BOND' and principal is not None else None
            result.append({'conid': row['conid'], 'name': definition.get('fullName') or row.get('contractDesc'),
                           'symbol': row.get('contractDesc'),
                           'asset_class': row.get('assetClass'), 'currency': row['currency'],
                           'quantity': quantity, 'face_value': face, 'market_value': market_value})
        return {'account': account, 'master': self.master, 'currency': currency, 'cash': cash, 'nav': nav,
                'collected_at': datetime.now(timezone.utc).isoformat(), 'positions': result}


def read_proposal_portfolio(account_id, contact_id):
    if not account_id or not contact_id:
        raise ServiceError('Choose a contact and linked account.', 400, code='portfolio_account_required')
    links = db.read(table='account_contact', query={'account_id': account_id, 'contact_id': contact_id}) or []
    accounts = db.read(table='account', query={'id': account_id}) or []
    if not links or len(accounts) != 1:
        raise ServiceError('Account is not linked to the selected contact.', 404, code='portfolio_account_unavailable')
    account = accounts[0]
    number, master = account.get('ibkr_account_number'), account.get('master_account')
    if not isinstance(number, str) or not re.fullmatch(r'U\d+', number):
        raise ServiceError('Account has no valid IBKR account number.', 422, code='portfolio_account_unavailable')
    return PortfolioReader(master).snapshot(number)
