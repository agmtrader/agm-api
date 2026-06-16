from flask import Blueprint, request
from src.components.clients.flagged_deposits import read_flagged_deposits, create_flagged_deposit
from src.utils.response import format_response

bp = Blueprint('flagged_deposits', __name__)

@bp.route('/create', methods=['POST'])
@format_response
def create():
    """Create a flagged deposit record for compliance or operations follow-up."""
    payload = request.get_json(force=True)  
    flagged_deposit = payload.get('flagged_deposit', None)
    return create_flagged_deposit(flagged_deposit=flagged_deposit)

@bp.route('/read', methods=['GET'])
@format_response
def read():
    """Read flagged deposit records filtered by id or account_id."""
    query = {}
    id = request.args.get('id', None)
    account_id = request.args.get('account_id', None)
    if id:
        query['id'] = id
    if account_id:
        query['account_id'] = account_id
        
    return read_flagged_deposits(query=query)
