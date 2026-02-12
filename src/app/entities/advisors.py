from flask import Blueprint, request
from src.components.entities.advisors import read_advisors, create_advisor
from src.utils.response import format_response

bp = Blueprint('advisors', __name__)

@bp.route('/create', methods=['POST'])
@format_response
def create_advisor_route():
    payload = request.get_json(force=True)  
    advisor = payload.get('advisor', None)
    return create_advisor(advisor)

@bp.route('/read', methods=['GET'])
@format_response
def advisors_route():
    query = {}
    id = request.args.get('id', None)
    code = request.args.get('code', None)
    contact_id = request.args.get('contact_id', None)
    if id:
        query['id'] = id
    if code:
        query['code'] = code
    if contact_id:
        query['contact_id'] = contact_id
    return read_advisors(query=query)