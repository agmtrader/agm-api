from flask import Blueprint, request
from src.components.clients.portfolio_plans import (
    create_portfolio_plan,
    read_portfolio_plans,
    update_portfolio_plan,
)
from src.utils.response import format_response

bp = Blueprint('portfolio_plans', __name__)


@bp.route('/create', methods=['POST'])
@format_response
def create():
    """Create a saved portfolio planning scenario linked to a risk profile."""
    payload = request.get_json(force=True)
    portfolio_plan = payload.get('portfolio_plan')
    if portfolio_plan is None:
        raise ValueError('Missing portfolio_plan payload')
    return create_portfolio_plan(portfolio_plan=portfolio_plan)


@bp.route('/read', methods=['GET'])
@format_response
def read():
    """Read saved portfolio plans filtered by id, risk_profile_id, or account_id."""
    query = {}
    id_ = request.args.get('id')
    risk_profile_id = request.args.get('risk_profile_id')
    account_id = request.args.get('account_id')
    if id_:
        query['id'] = id_
    if risk_profile_id:
        query['risk_profile_id'] = risk_profile_id
    if account_id:
        query['account_id'] = account_id
    return read_portfolio_plans(query=query)


@bp.route('/update', methods=['POST'])
@format_response
def update():
    """Update a saved portfolio plan selected by id or risk_profile_id."""
    payload = request.get_json(force=True)
    portfolio_plan = payload.get('portfolio_plan')
    query = payload.get('query')
    if query is None:
        raise ValueError('Missing query payload')
    if portfolio_plan is None:
        raise ValueError('Missing portfolio_plan payload')
    return update_portfolio_plan(query=query, portfolio_plan=portfolio_plan)
