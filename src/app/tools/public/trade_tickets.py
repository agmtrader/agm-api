from flask import request, Blueprint
from src.components.tools.public.trade_tickets import list_trade_tickets, read, generate, send_trade_ticket_email
from src.utils.response import format_response

bp = Blueprint('trade_tickets', __name__)

@bp.route('/send_email', methods=['POST'])
@format_response
def send_email_route():
    payload = request.get_json(force=True)
    return send_trade_ticket_email(payload['content'], payload['client_email'])

@bp.route('/list', methods=['GET'])
@format_response
def list_route():
    """Read trade ticket records filtered by id or user_id."""
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
    """Read a generated trade ticket payload by query_id."""
    query_id = request.args.get('query_id', None)
    return read(query_id)

@bp.route('/generate', methods=['POST'])
@format_response
def generate_route():
    """Generate trade ticket output from a flex query payload and a list of selected indices."""
    payload = request.get_json(force=True)
    query_id = payload['query_id']
    indices = payload['indices'].split(',')
    indices = [int(index) for index in indices]
    flex_query_dict = payload['flex_query_dict']
    return generate(query_id=query_id, flex_query_dict=flex_query_dict, indices=indices)
