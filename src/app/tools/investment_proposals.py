from flask import Blueprint, request
from src.components.tools.investment_proposals import create_investment_proposal, read_investment_proposals
from src.utils.managers.scope_manager import verify_scope
from src.utils.response import format_response

bp = Blueprint('investment_proposals', __name__)

@bp.route('/create', methods=['POST'])
@verify_scope('investment_proposals/create')
@format_response
def create_route():
    payload = request.get_json(force=True)
    risk_profile = payload.get('risk_profile', None)
    return create_investment_proposal(risk_profile=risk_profile)

@bp.route('/read', methods=['GET'])
@verify_scope('investment_proposals/read')
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