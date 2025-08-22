from flask import Blueprint
from src.components.tools.email_change import read_email_change
from src.utils.managers.scope_manager import verify_scope
from src.utils.response import format_response

bp = Blueprint('email_change', __name__)

@bp.route('/read', methods=['GET'])
@verify_scope('email_change/read')
@format_response        
def read_route():
    return read_email_change()