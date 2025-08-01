from flask import request, Blueprint
from src.components.tools.trade_tickets import generate_trade_ticket, generate_client_confirmation_message, list_trade_tickets, fetch_trade_ticket
from src.utils.managers.scope_manager import verify_scope
from src.utils.response import format_response

bp = Blueprint('trade_tickets', __name__)

@bp.route('/list', methods=['POST'])
@verify_scope('trade_tickets/list')
@format_response
def list():
    payload = request.get_json(force=True)
    query = payload.get('query', {})
    return list_trade_tickets(query=query)

@bp.route('/fetch', methods=['POST'])
@verify_scope('trade_tickets/fetch')
@format_response
def fetch_trade_ticket_route():
    payload = request.get_json(force=True)
    query_id = payload['query_id']
    return fetch_trade_ticket(query_id)

@bp.route('/generate_trade_ticket', methods=['POST'])
@verify_scope('trade_tickets/generate_trade_ticket')
@format_response
def generate_trade_ticket_route():
    payload = request.get_json(force=True)
    indices = payload['indices'].split(',')
    indices = [int(index) for index in indices]
    flex_query_dict = payload['flex_query_dict']
    return generate_trade_ticket(flex_query_dict=flex_query_dict, indices=indices)

@bp.route('/generate_client_confirmation_message', methods=['POST'])
@verify_scope('trade_tickets/generate_client_confirmation_message')
@format_response
def generate_client_confirmation_message_route():
    payload = request.get_json(force=True)
    trade_data = payload['trade_data']
    return generate_client_confirmation_message(trade_data)