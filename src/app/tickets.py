
from flask import Blueprint, request
from src.components.tickets import read_tickets, update_ticket, create_ticket
from src.utils.scope_manager import verify_scope

bp = Blueprint('tickets', __name__)

@bp.route('/create', methods=['POST'])
@verify_scope('tickets/create')
def create_route():
    payload = request.get_json(force=True)
    data = payload['data']
    id = payload['id']
    return create_ticket(data=data, id=id)

@bp.route('/read', methods=['POST'])
@verify_scope('tickets/read')
def read_route():
    payload = request.get_json(force=True)
    query = payload['query']
    return read_tickets(query=query)

@bp.route('/update', methods=['POST'])
@verify_scope('tickets/update')
def update_route():
    payload = request.get_json(force=True)
    data = payload['data']
    query = payload['query']
    return update_ticket(data=data, query=query)