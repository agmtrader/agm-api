from flask import Blueprint
from src.components.tools.tws import get_account_summary
from src.utils.response import format_response

bp = Blueprint('tws', __name__)

@bp.route('/account/summary', methods=['POST'])
@format_response
def get_account_summary_route():
    return get_account_summary()