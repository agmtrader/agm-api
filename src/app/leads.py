from flask import Blueprint, request
from src.components.leads import read_leads, update_lead, create_lead
from src.utils.scope_manager import verify_scope, enforce_user_filter

bp = Blueprint('leads', __name__)

@bp.route('/create', methods=['POST'])
@verify_scope('leads/create')
@enforce_user_filter()
def create_route():
    payload = request.get_json(force=True)
    data = payload['data']
    id = payload['id']
    return create_lead(data=data, id=id)

@bp.route('/read', methods=['POST'])
@verify_scope('leads/read')
@enforce_user_filter()
def read_route():
    payload = request.get_json(force=True)
    query = payload.get('query', None)
    return read_leads(query=query)

@bp.route('/update', methods=['POST'])
@verify_scope('leads/update')
@enforce_user_filter()
def update_route():
    payload = request.get_json(force=True)
    data = payload['data']
    query = payload.get('query', None)
    return update_lead(data=data, query=query)