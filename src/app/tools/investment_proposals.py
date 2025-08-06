from flask import Blueprint
from src.components.tools.investment_proposals import generate_investment_proposal
from src.utils.managers.scope_manager import verify_scope
from src.utils.response import format_response

bp = Blueprint('investment_proposals', __name__)

@bp.route('/generate', methods=['GET'])
@verify_scope('investment_proposals/generate')
@format_response
def generate_route():
    return generate_investment_proposal()
