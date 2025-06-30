from flask import Blueprint
from src.components.rtd import read_rtd
from src.utils.managers.scope_manager import verify_scope
from src.utils.response import format_response

bp = Blueprint('rtd', __name__)
 
@bp.route('/read', methods=['POST'])
@verify_scope('accounts/read')
@format_response
def read_route():
    return read_rtd()