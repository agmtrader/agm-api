from flask import Blueprint, request
from src.components.management_type_requests import (
    create_management_type_request,
    read_management_type_requests,
)
from src.utils.response import format_response

bp = Blueprint('management_type_requests', __name__)


@bp.route('/create', methods=['POST'])
@format_response
def create():
    """Endpoint to create a new management_type_request record.

    Expects JSON body like:
    {
        "management_type_request": { ... }
    }
    """
    payload = request.get_json(force=True)
    management_type_request = payload.get('management_type_request')
    if management_type_request is None:
        raise ValueError('Missing management_type_request payload')
    return create_management_type_request(
        management_type_request=management_type_request,
    )


@bp.route('/read', methods=['GET'])
@format_response
def read():
    """Endpoint to read management_type_request records.

    Optional query params: id, account_id
    """
    query = {}
    id_ = request.args.get('id')
    account_id = request.args.get('account_id')
    if id_:
        query['id'] = id_
    if account_id:
        query['account_id'] = account_id
    return read_management_type_requests(query=query)
