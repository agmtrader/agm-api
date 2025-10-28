from flask import request, Blueprint
from src.components.tools.trade_tickets import list_trade_tickets, read, generate_trade_confirmation_message
from src.utils.response import format_response

bp = Blueprint('trade_tickets', __name__)

@bp.route('/list', methods=['GET'])
@format_response
def list_route():
    query = {}
    id = request.args.get('id', None)
    user_id = request.args.get('user_id', None)
    if id:
        query['id'] = id
    if user_id:
        query['user_id'] = user_id
    return list_trade_tickets(query=query)

@bp.route('/read', methods=['GET'])
@format_response
def read_route():
    query_id = request.args.get('query_id', None)
    return read(query_id)

@bp.route('/confirmation_message', methods=['POST'])
@format_response
def confirmation_message_route():
    payload = request.get_json(force=True)
    indices = payload['indices'].split(',')
    indices = [int(index) for index in indices]
    flex_query_dict = payload['flex_query_dict']
    return generate_trade_confirmation_message(flex_query_dict=flex_query_dict, indices=indices)