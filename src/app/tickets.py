from flask import Blueprint, request, jsonify
from src.components.tickets import read_tickets, update_ticket, create_ticket
from src.utils.scope_manager import verify_scope, enforce_user_filter
from flask_jwt_extended import get_jwt, get_jwt_identity
from src.utils.logger import logger

bp = Blueprint('tickets', __name__)

@bp.route('/create', methods=['POST'])
@verify_scope('tickets/create')
@enforce_user_filter()
def create_route():
    payload = request.get_json(force=True)
    data = payload['data']
    id = payload['id']
    return create_ticket(data=data, id=id)

@bp.route('/read', methods=['POST'])
@verify_scope('tickets/read')
@enforce_user_filter()
def read_route():
    payload = request.get_json(force=True)
    query = payload.get('query', None)
    return read_tickets(query=query)

@bp.route('/update', methods=['POST'])
@verify_scope('tickets/update')
@enforce_user_filter()
def update_route():
    payload = request.get_json(force=True)
    data = payload['data']
    query = payload.get('query', None)
    return update_ticket(data=data, query=query)