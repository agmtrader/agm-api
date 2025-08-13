from flask import Blueprint, request
from src.components.tools.investment_proposals import create_investment_proposal
from src.utils.managers.scope_manager import verify_scope
from src.utils.response import format_response

bp = Blueprint('investment_proposals', __name__)

@bp.route('/create', methods=['GET'])
@verify_scope('investment_proposals/create')
@format_response
def create_route():
    risk_profile_id = request.args.get('risk_profile_id', None)
    return create_investment_proposal(risk_profile_id=risk_profile_id)