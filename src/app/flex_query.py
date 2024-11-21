from flask import Blueprint, request
from src.components.flex_query import fetchFlexQueries

bp = Blueprint('flex_query', __name__)

@bp.route('/fetch', methods=['POST'])
def fetch_flex_queries():
    data = request.get_json(force=True)
    queryIds = data['queryIds']
    response = fetchFlexQueries(queryIds)
    return response