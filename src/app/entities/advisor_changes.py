from flask import Blueprint, request
from src.components.entities.advisor_changes import create_advisor_change, read_advisor_changes
from src.utils.response import format_response

bp = Blueprint('advisor_changes', __name__)

@bp.route('/create', methods=['POST'])
@format_response
def create_route():
    payload = request.get_json(force=True)
    advisor_change = payload.get('advisor_change', None)
    return create_advisor_change(advisor_change=advisor_change)

@bp.route('/read', methods=['GET'])
@format_response
def read_route():
    query = {}
    id = request.args.get('id', None)
    account_id = request.args.get('account_id', None)
    if id:
        query['id'] = id
    if account_id:
        query['account_id'] = account_id
    return read_advisor_changes(query=query)
