from flask import Blueprint, request
from src.components.tools.message_center import get_message_center_emails
from src.utils.managers.scope_manager import verify_scope
from src.utils.response import format_response

bp = Blueprint('message_center', __name__)

@bp.route('/read', methods=['GET'])
@verify_scope('message_center/read')
@format_response
def read_route():
    return get_message_center_emails()