from flask import request, Blueprint
from src.components.tools.trade_tickets import list_trade_tickets, read, generate_trade_confirmation_message
from src.utils.managers.scope_manager import verify_scope
from src.utils.response import format_response

bp = Blueprint('trade_tickets', __name__)

@bp.route('/list', methods=['GET'])
@verify_scope('trade_tickets/list')
@format_response
def list_route():
    return list_trade_tickets()

@bp.route('/read', methods=['GET'])
@verify_scope('trade_tickets/read')
@format_response
def read_route():
    query_id = request.args.get('query_id', None)
    return read(query_id)

@bp.route('/confirmation_message', methods=['POST'])
@verify_scope('trade_tickets/confirmation_message')
@format_response
def confirmation_message_route():
    payload = request.get_json(force=True)
    indices = payload['indices'].split(',')
    indices = [int(index) for index in indices]
    flex_query_dict = payload['flex_query_dict']
    return generate_trade_confirmation_message(flex_query_dict=flex_query_dict, indices=indices)