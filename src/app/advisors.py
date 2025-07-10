from flask import Blueprint, request
from src.components.advisors import read_advisors, create_advisor
from src.utils.managers.scope_manager import verify_scope
from src.utils.response import format_response

bp = Blueprint('advisors', __name__)

@bp.route('/create', methods=['POST'])
@verify_scope('advisors/create')
@format_response
def create_advisor_route():
    payload = request.get_json(force=True)
    advisor = payload.get('advisor', None)
    return create_advisor(advisor)

@bp.route('/read', methods=['POST'])
@verify_scope('advisors/read')
@format_response
def advisors_route():
    payload = request.get_json(force=True)
    query = payload.get('query', None)
    return read_advisors(query)