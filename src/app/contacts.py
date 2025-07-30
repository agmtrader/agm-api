from flask import Blueprint, request
from src.components.contacts import read_contacts, update_contact, create_contact
from src.utils.managers.scope_manager import verify_scope
from src.utils.response import format_response

bp = Blueprint('contacts', __name__)

@bp.route('/create', methods=['POST'])
@verify_scope('contacts/create')
@format_response
def create_contact_route():
    payload = request.get_json(force=True)
    contact = payload.get('contact', None)
    return create_contact(contact=contact)

@bp.route('/read', methods=['POST'])
@verify_scope('contacts/read')
@format_response
def read_contacts_route():
    payload = request.get_json(force=True)
    query = payload.get('query', None)
    return read_contacts(query=query)

@bp.route('/update', methods=['POST'])
@verify_scope('contacts/update')
@format_response
def update_contact_route():
    payload = request.get_json(force=True)
    contact = payload.get('contact', None)
    query = payload.get('query', None)
    return update_contact(query=query, contact=contact)