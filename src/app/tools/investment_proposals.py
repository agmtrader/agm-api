from flask import Blueprint, request
from src.components.tools.investment_proposals import create_investment_proposal
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