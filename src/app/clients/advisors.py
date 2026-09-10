from flask import Blueprint, request
from src.components.clients.advisors import (
    create_advisor,
    read_advisors,
    read_current_advisor_account_contacts,
    read_current_advisor_account_proposals,
    read_current_advisor_account_statement,
    read_current_advisor_accounts,
    read_current_advisor_nav,
    read_current_advisor_open_positions,
    update_advisor,
)
from src.utils.response import format_response

bp = Blueprint('advisors', __name__)

@bp.route('/create', methods=['POST'])
@format_response
def create_advisor_route():
    """Create an advisor record."""
    payload = request.get_json(force=True)  
    advisor = payload.get('advisor', None)
    return create_advisor(advisor)

@bp.route('/update', methods=['POST'])
@format_response
def update_advisor_route():
    """Update an advisor record selected by the provided query payload."""
    payload = request.get_json(force=True)
    return update_advisor(
        query=payload.get('query'),
        advisor=payload.get('advisor'),
    )

@bp.route('/read', methods=['GET'])
@format_response
def advisors_route():
    """Read advisors from the database filtered by id, advisor code, or contact_id."""
    query = {}
    id = request.args.get('id', None)
    code = request.args.get('code', None)
    contact_id = request.args.get('contact_id', None)
    if id:
        query['id'] = id
    if code:
        query['code'] = code
    if contact_id:
        query['contact_id'] = contact_id
    return read_advisors(query=query)


@bp.route('/me/accounts', methods=['GET'])
@format_response
def current_advisor_accounts_route():
    """Read managed accounts for the authenticated advisor only."""
    return read_current_advisor_accounts()


@bp.route('/me/open_positions', methods=['GET'])
@format_response
def current_advisor_open_positions_route():
    """Read open positions for the authenticated advisor only."""
    return read_current_advisor_open_positions()


@bp.route('/me/nav', methods=['GET'])
@format_response
def current_advisor_nav_route():
    """Read monthly NAV rows for the authenticated advisor only."""
    years = request.args.get('years', request.args.get('year', '')).split(',')
    months = request.args.get('months', request.args.get('month', '')).split(',')
    years = [year.strip() for year in years if year.strip()]
    months = [month.strip() for month in months if month.strip()]
    return read_current_advisor_nav(years, months)


@bp.route('/me/account_contacts', methods=['GET'])
@format_response
def current_advisor_account_contacts_route():
    """Read contact links for an account owned by the authenticated advisor."""
    return read_current_advisor_account_contacts(request.args.get('account_id'))


@bp.route('/me/account_proposals', methods=['GET'])
@format_response
def current_advisor_account_proposals_route():
    """Read proposals for holders of an advisor-owned account."""
    return read_current_advisor_account_proposals(request.args.get('account_id'))


@bp.route('/me/account_statement', methods=['POST'])
@format_response
def current_advisor_account_statement_route():
    """Read a statement for an advisor-owned account."""
    payload = request.get_json(force=True) or {}
    return read_current_advisor_account_statement(
        account_id=payload.get('account_id'),
        start_date=payload.get('start_date'),
        end_date=payload.get('end_date'),
        language=payload.get('language', 'en'),
    )
