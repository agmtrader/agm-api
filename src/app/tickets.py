from flask import Blueprint, request
from src.components.tickets import read_tickets, update_ticket_info, create_ticket, read_ticket_info, upload_ticket_poa, read_ticket_docs, upload_ticket_poi
from src.utils.managers.scope_manager import verify_scope, enforce_user_filter
from src.utils.response import format_response

bp = Blueprint('tickets', __name__)

@bp.route('/create', methods=['POST'])
@verify_scope('tickets/create')
@enforce_user_filter()
@format_response
def create_route():
    payload = request.get_json(force=True)
    ticket = payload.get('ticket', None)
    ticket_info = payload.get('info', None)
    return create_ticket(ticket=ticket, ticket_info=ticket_info)

@bp.route('/read', methods=['POST'])
@verify_scope('tickets/read')
@enforce_user_filter()
@format_response
def read_route():
    payload = request.get_json(force=True)
    query = payload.get('query', None)
    return read_tickets(query=query)

@bp.route('/read_info', methods=['POST'])
@verify_scope('tickets/read')
@enforce_user_filter()
@format_response
def read_info_route():
    payload = request.get_json(force=True)
    query = payload.get('query', None)
    return read_ticket_info(query=query)

@bp.route('/read_documents', methods=['POST'])
@verify_scope('tickets/read')
@enforce_user_filter()
@format_response
def read_documents_route():
    payload = request.get_json(force=True)
    ticket_id = payload.get('ticket_id', None)
    query = payload.get('query', None)
    return read_ticket_docs(ticket_id=ticket_id, query=query)

@bp.route('/update_info', methods=['POST'])
@verify_scope('tickets/update')
@enforce_user_filter()
@format_response
def update_info_route():
    payload = request.get_json(force=True)
    query = payload.get('query', None)
    ticket_info = payload.get('ticket_info', None)
    return update_ticket_info(ticket_info=ticket_info, query=query)

@bp.route('/upload_poa', methods=['POST'])
@verify_scope('tickets/upload')
@enforce_user_filter()
@format_response
def upload_poa_route():
    payload = request.get_json(force=True)
    f = payload.get('f', None)
    document_info = payload.get('document_info', None)
    user_id = payload.get('user_id', None)
    ticket_id = payload.get('ticket_id', None)
    ticket_info = payload.get('ticket_info', None)
    return upload_ticket_poa(f=f, document_info=document_info, user_id=user_id, ticket_id=ticket_id, ticket_info=ticket_info)

@bp.route('/upload_poi', methods=['POST'])
@verify_scope('tickets/upload')
@enforce_user_filter()
@format_response
def upload_poi_route():
    payload = request.get_json(force=True)
    f = payload.get('f', None)
    document_info = payload.get('document_info', None)
    user_id = payload.get('user_id', None)
    ticket_id = payload.get('ticket_id', None)
    ticket_info = payload.get('ticket_info', None)
    return upload_ticket_poi(f=f, document_info=document_info, user_id=user_id, ticket_id=ticket_id, ticket_info=ticket_info)