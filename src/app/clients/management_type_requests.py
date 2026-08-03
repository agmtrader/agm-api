from flask import Blueprint, request
from src.components.clients.management_type_requests import create_management_type_request
from src.utils.response import format_response

bp = Blueprint('management_type_requests', __name__)


@bp.route('/create', methods=['POST'])
@format_response
def create():
    """Create a management type change request for an account."""
    payload = request.get_json(force=True)
    management_type_request = payload.get('management_type_request')
    if management_type_request is None:
        raise ValueError('Missing management_type_request payload')
    return create_management_type_request(
        management_type_request=management_type_request,
    )

