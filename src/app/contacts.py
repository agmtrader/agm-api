from flask import Blueprint, request
from src.components.contacts import read_contacts, update_contact, create_contact
from src.utils.scope_manager import verify_scope, enforce_user_filter

bp = Blueprint('contacts', __name__)

@bp.route('/create', methods=['POST'])
@verify_scope('contacts/create')
@enforce_user_filter(field_name='id')
def create_contact_route():
    payload = request.get_json(force=True)
    data = payload.get('data', None)
    id = payload.get('id', None)
    return create_contact(data=data, id=id)

@bp.route('/read', methods=['POST'])
@verify_scope('contacts/read')
@enforce_user_filter(field_name='id')
def read_contacts_route():
    payload = request.get_json(force=True)
    query = payload.get('query', None)
    return read_contacts(query=query)

@bp.route('/update', methods=['POST'])
@verify_scope('contacts/update')
@enforce_user_filter(field_name='id')
def update_contact_route():
    payload = request.get_json(force=True)
    data = payload.get('data', None)
    query = payload.get('query', None)
    return update_contact(data=data, query=query)

@bp.route('/delete', methods=['POST'])
@verify_scope('contacts/delete')
@enforce_user_filter(field_name='id')
def delete_contact_route():
    payload = request.get_json(force=True)