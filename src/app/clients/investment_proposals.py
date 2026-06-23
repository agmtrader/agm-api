from flask import Blueprint, request
from src.components.clients.investment_proposals import (
    create_investment_proposal_with_assets,
    create_investment_proposal_with_portfolio_plan,
    create_investment_proposal_with_risk_profile,
    preview_investment_proposal_with_portfolio_plan,
    read_investment_proposals,
)
from src.utils.response import format_response

bp = Blueprint('investment_proposals', __name__)

@bp.route('/create/risk', methods=['POST'])
@format_response
def create_with_risk_profile_route():
    """Create an investment proposal from a risk profile payload."""
    payload = request.get_json(force=True)
    risk_profile = payload.get('risk_profile', None)
    return create_investment_proposal_with_risk_profile(risk_profile=risk_profile)

@bp.route('/create/assets', methods=['POST'])
@format_response
def create_with_assets_route():
    """Create an investment proposal from an explicit asset allocation payload."""
    payload = request.get_json(force=True)
    assets = payload.get('assets', None)
    return create_investment_proposal_with_assets(assets=assets)


@bp.route('/create/plan', methods=['POST'])
@format_response
def create_with_portfolio_plan_route():
    """Create an investment proposal from a saved portfolio plan payload."""
    payload = request.get_json(force=True)
    portfolio_plan = payload.get('portfolio_plan', None)
    return create_investment_proposal_with_portfolio_plan(portfolio_plan=portfolio_plan)


@bp.route('/preview/plan', methods=['POST'])
@format_response
def preview_with_portfolio_plan_route():
    """Preview an investment proposal from a portfolio plan payload without persisting it."""
    payload = request.get_json(force=True)
    portfolio_plan = payload.get('portfolio_plan', None)
    return preview_investment_proposal_with_portfolio_plan(portfolio_plan=portfolio_plan)

@bp.route('/read', methods=['GET'])
@format_response
def read_route():
    """Read investment proposals filtered by proposal id, risk_profile_id, or account_id."""
    query = {}
    id = request.args.get('id', None)
    risk_profile_id = request.args.get('risk_profile_id', None)
    account_id = request.args.get('account_id', None)
    if id:
        query['id'] = id
    if risk_profile_id:
        query['risk_profile_id'] = risk_profile_id
    if account_id:
        query['account_id'] = account_id
    return read_investment_proposals(query=query)
