from flask import request, Blueprint, g, send_file
from src.components.tools.public.trade_tickets import list_trade_tickets, read, generate, generate_improsa_xls, send_trade_ticket_email
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
    scopes = getattr(g, 'current_user_scopes', set())
    if 'all' in scopes and user_id:
        query['user_id'] = user_id
    else:
        query['user_id'] = str(g.current_user['id'])
    return list_trade_tickets(query=query)

@bp.route('/read', methods=['GET'])
@format_response
def read_route():
    """Read a generated trade ticket payload by query_id."""
    query_id = request.args.get('query_id', None)
    if not query_id:
        return {'error': 'query_id is required'}, 400
    scopes = getattr(g, 'current_user_scopes', set())
    owner_query = {'query_id': query_id}
    if 'all' not in scopes:
        owner_query['user_id'] = str(g.current_user['id'])
    if not list_trade_tickets(query=owner_query):
        return {'error': 'Trade ticket not found'}, 404
    return read(query_id)

@bp.route('/generate', methods=['POST'])
@format_response
def generate_route():
    """Generate trade ticket output from a flex query payload and a list of selected indices."""
    payload = request.get_json(force=True)
    indices = payload['indices'].split(',')
    indices = [int(index) for index in indices]
    flex_query_dict = payload['flex_query_dict']
    return generate(flex_query_dict=flex_query_dict, indices=indices)


@bp.route('/export_improsa', methods=['POST'])
@format_response
def export_improsa_route():
    payload = request.get_json(force=True)
    query_id = payload.get('query_id')
    if not query_id:
        return {'error': 'query_id is required'}, 400
    scopes = getattr(g, 'current_user_scopes', set())
    owner_query = {'query_id': query_id}
    if 'all' not in scopes:
        owner_query['user_id'] = str(g.current_user['id'])
    if not list_trade_tickets(query=owner_query):
        return {'error': 'Trade ticket not found'}, 404

    execution_ids = payload.get('execution_ids') or []
    if not isinstance(execution_ids, list) or not execution_ids or not all(isinstance(value, str) and value for value in execution_ids):
        return {'error': 'execution_ids must be a non-empty list of execution identifiers'}, 400
    flex_query_dict = read(query_id)
    if not isinstance(flex_query_dict, list):
        return {'error': 'Unable to read the trade ticket executions'}, 502
    rows_by_execution_id = {
        str(row.get('ExecID')): index
        for index, row in enumerate(flex_query_dict or [])
        if row.get('ExecID') not in (None, '')
    }
    try:
        indices = [rows_by_execution_id[execution_id] for execution_id in execution_ids]
    except KeyError as exc:
        return {'error': f'Execution not found: {exc.args[0]}'}, 400
    try:
        workbook = generate_improsa_xls(flex_query_dict, indices)
    except ValueError as exc:
        return {'error': str(exc)}, 400
    return send_file(
        workbook,
        as_attachment=True,
        download_name='improsa-trade-ticket.xls',
        mimetype='application/vnd.ms-excel',
    )
