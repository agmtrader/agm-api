from flask import Blueprint, request
from src.components.advisors import read_advisors
from src.utils.scope_manager import verify_scope

bp = Blueprint('advisors', __name__)

@bp.route('/read', methods=['POST'])
@verify_scope('advisors/read')
def advisors_route():
    body = request.get_json(force=True)
    query = body['query']
    return read_advisors(query)