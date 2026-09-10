from src.utils.exception import ServiceError, handle_exception
from src.utils.connectors.supabase import db
from src.utils.logger import logger
from src.utils.authz import get_current_advisor
from src.components.clients.accounts import get_account_statements, read_account_contacts, read_accounts
from src.components.clients.investment_proposals import read_investment_proposals
from src.components.tools.public.reporting import get_clients_report, get_nav_report_monthly, get_open_positions_report

logger.announcement('Initializing Advisors Service', type='info')
logger.announcement('Initialized Advisors Service', type='success')

@handle_exception
def create_advisor(advisor: dict = None):
    advisor_id = db.create(table='advisor', data=advisor)
    return {'id': advisor_id}

@handle_exception
def update_advisor(query: dict = None, advisor: dict = None):
    """Update an advisor record selected by query."""
    if query is None:
        raise ServiceError('Query must be provided', status_code=400)
    if advisor is None:
        raise ServiceError('Advisor must be provided', status_code=400)
    unsupported_fields = set(advisor) - {'contact_id'}
    if unsupported_fields:
        raise ServiceError('Only contact_id can be updated through this route', status_code=400)

    advisor_id = db.update(table='advisor', query=query, data=advisor)
    return {'id': advisor_id}

@handle_exception
def read_advisors(query=None):
    advisors = db.read(table='advisor', query=query)
    return advisors


def _current_advisor_accounts() -> list:
    advisor = get_current_advisor()
    if not advisor or advisor.get('code') is None:
        raise ServiceError('Advisor ownership could not be resolved', status_code=403)
    return read_accounts(query={'advisor_code': advisor['code']})


def _current_advisor_account(account_id: str) -> dict:
    if not account_id:
        raise ServiceError('account_id is required', status_code=400)
    account = next(
        (item for item in _current_advisor_accounts() if str(item.get('id')) == str(account_id)),
        None,
    )
    if not account:
        raise ServiceError('Account not found', status_code=404)
    return account


@handle_exception
def read_current_advisor_accounts() -> list:
    """Return only accounts belonging to the authenticated advisor."""
    accounts = _current_advisor_accounts()
    try:
        clients = get_clients_report() or []
    except Exception:
        clients = []
    titles = {
        str(client.get('Account ID')).strip(): client.get('Title') or '-'
        for client in clients
        if client.get('Account ID') is not None
    }
    return [
        {**account, 'title': titles.get(str(account.get('ibkr_account_number') or '').strip(), '-')}
        for account in accounts
    ]


@handle_exception
def read_current_advisor_open_positions() -> list:
    """Return open positions only for accounts owned by the advisor."""
    accounts = _current_advisor_accounts()
    account_ids = {
        str(account.get('ibkr_account_number') or '').strip()
        for account in accounts
        if str(account.get('ibkr_account_number') or '').strip()
    }
    positions = get_open_positions_report() or []
    return [
        position for position in positions
        if str(position.get('ClientAccountID') or '').strip() in account_ids
        and str(position.get('OpenDateTime') or '').strip()
    ]


@handle_exception
def read_current_advisor_nav(years: list, months: list) -> list:
    """Return monthly NAV rows only for accounts owned by the advisor."""
    accounts = _current_advisor_accounts()
    account_ids = {
        str(account.get('ibkr_account_number') or '').strip()
        for account in accounts
        if str(account.get('ibkr_account_number') or '').strip()
    }
    rows = get_nav_report_monthly(years, months) or []
    return [
        row for row in rows
        if str(
            row.get('ClientAccountID')
            or row.get('Account ID')
            or row.get('account_id')
            or ''
        ).strip() in account_ids
    ]


@handle_exception
def read_current_advisor_account_contacts(account_id: str) -> list:
    """Return contact links only for an account owned by the advisor."""
    _current_advisor_account(account_id)
    return read_account_contacts(query={'account_id': account_id})


@handle_exception
def read_current_advisor_account_proposals(account_id: str) -> list:
    """Return proposals for holders of an advisor-owned account."""
    _current_advisor_account(account_id)
    links = read_account_contacts(query={'account_id': account_id})
    contact_ids = {str(link.get('contact_id')) for link in links if link.get('contact_id')}
    proposals = []
    seen_ids = set()
    for contact_id in contact_ids:
        for proposal in read_investment_proposals(query={'contact_id': contact_id}) or []:
            proposal_id = str(proposal.get('id'))
            if proposal_id not in seen_ids:
                seen_ids.add(proposal_id)
                proposals.append(proposal)
    return sorted(proposals, key=lambda proposal: str(proposal.get('created') or ''), reverse=True)


@handle_exception
def read_current_advisor_account_statement(
    account_id: str,
    start_date: str,
    end_date: str,
    language: str = 'en',
) -> dict:
    """Read a statement for an advisor-owned account using its stored master."""
    account = _current_advisor_account(account_id)
    master_account = account.get('master_account')
    if not master_account:
        raise ServiceError('Account master is not configured', status_code=409)
    if not start_date or not end_date:
        raise ServiceError('start_date and end_date are required', status_code=400)
    if language not in {'en', 'es'}:
        raise ServiceError('Invalid language. Supported values: en, es', status_code=400)
    return get_account_statements(
        account_id=account.get('ibkr_account_number'),
        start_date=start_date,
        end_date=end_date,
        master_account=master_account,
        language=language,
    )
