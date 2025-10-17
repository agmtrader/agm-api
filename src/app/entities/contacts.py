from flask import Blueprint, request
from src.components.entities.contacts import read_contacts, create_contact, update_contact
from src.utils.response import format_response

bp = Blueprint('contacts', __name__)

@bp.route('/create', methods=['POST'])
@format_response
def create_contact_route():
    payload = request.get_json(force=True)  
    contact = payload.get('contact', None)
    return create_contact(contact)

@bp.route('/read', methods=['GET'])
@format_response
def contacts_route():
    query = {}
    id = request.args.get('id', None)
    email = request.args.get('email', None)
    if id:
        query['id'] = id
    if email:
        query['email'] = email
    return read_contacts(query=query)

@bp.route('/update', methods=['POST'])
@format_response
def update_contact_route():
    payload = request.get_json(force=True)  
    contact = payload.get('contact', None)
    query = payload.get('query', None)
    return update_contact(query=query, contact=contact)