from flask import Blueprint, request
from src.components.flex_query import fetchFlexQueries
from src.utils.scope_manager import verify_scope

bp = Blueprint('flex_query', __name__)

@bp.route('/fetch', methods=['POST'])
@verify_scope('flex_query/fetch')
def fetch_flex_queries():
    data = request.get_json(force=True)
    queryIds = data['queryIds']
    return fetchFlexQueries(queryIds)