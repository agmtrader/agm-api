from flask import Blueprint, request

from src.components.clients.account_contacts import (
    create_account_contact,
    read_account_contacts,
    update_account_contact,
    delete_account_contact,
)
from src.utils.response import format_response

bp = Blueprint('account_contacts', __name__)


@bp.route('/create', methods=['POST'])
@format_response
def create_account_contact_route():
    """Create a relationship record between an account and a contact."""
    payload = request.get_json(force=True)
    account_contact = payload.get('account_contact', None)
    return create_account_contact(account_contact=account_contact)


@bp.route('/read', methods=['GET'])
@format_response
def read_account_contacts_route():
    """Read account-contact relationship records filtered by id, account, contact, or entity."""
    query = {}
    id = request.args.get('id', None)
    account_id = request.args.get('account_id', None)
    contact_id = request.args.get('contact_id', None)
    entity_id = request.args.get('entity_id', None)

    if id:
        query['id'] = id
    if account_id:
        query['account_id'] = account_id
    if contact_id:
        query['contact_id'] = contact_id
    if entity_id:
        query['entity_id'] = entity_id

    return read_account_contacts(query=query)


@bp.route('/update', methods=['POST'])
@format_response
def update_account_contact_route():
    """Update an account-contact relationship selected by the incoming query payload."""
    payload = request.get_json(force=True)
    query = payload.get('query', None)
    account_contact = payload.get('account_contact', None)
    return update_account_contact(query=query, account_contact=account_contact)


@bp.route('/delete', methods=['POST'])
@format_response
def delete_account_contact_route():
    """Delete account-contact relationship records that match the provided query payload."""
    payload = request.get_json(force=True)
    query = payload.get('query', None)
    return delete_account_contact(query=query)
