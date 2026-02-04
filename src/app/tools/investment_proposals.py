from flask import Blueprint, request
from src.components.tools.investment_proposals import (
    create_investment_proposal_with_assets,
    create_investment_proposal_with_risk_profile,
    read_investment_proposals,
)
from src.utils.response import format_response

bp = Blueprint('investment_proposals', __name__)

@bp.route('/create/risk-profile', methods=['POST'])
@format_response
def create_with_risk_profile_route():
    payload = request.get_json(force=True)
    risk_profile = payload.get('risk_profile', None)
    return create_investment_proposal_with_risk_profile(risk_profile=risk_profile)

@bp.route('/create/assets', methods=['POST'])
@format_response
def create_with_assets_route():
    payload = request.get_json(force=True)
    assets = payload.get('assets', None)
    return create_investment_proposal_with_assets(assets=assets)

@bp.route('/read', methods=['GET'])
@format_response
def read_route():
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