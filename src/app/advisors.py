from flask import Blueprint, request
from src.components.advisors import read_advisors, create_advisor
from src.utils.managers.scope_manager import verify_scope

bp = Blueprint('advisors', __name__)

@bp.route('/create', methods=['POST'])
@verify_scope('advisors/create')
def create_advisor_route():
    payload = request.get_json(force=True)
    advisor = payload.get('advisor', None)
    return create_advisor(advisor)

@bp.route('/read', methods=['POST'])
@verify_scope('advisors/read')
def advisors_route():
    payload = request.get_json(force=True)
    query = payload.get('query', None)
    return read_advisors(query)