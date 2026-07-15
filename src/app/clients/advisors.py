from flask import Blueprint, request
from src.components.clients.advisors import (
    create_advisor,
    create_and_link_advisor_contact,
    link_advisor_contact,
    read_advisors,
)
from src.utils.response import format_response

bp = Blueprint('advisors', __name__)

@bp.route('/create', methods=['POST'])
@format_response
def create_advisor_route():
    """Create an advisor record."""
    payload = request.get_json(force=True)  
    advisor = payload.get('advisor', None)
    return create_advisor(advisor)

@bp.route('/read', methods=['GET'])
@format_response
def advisors_route():
    """Read advisors from the database filtered by id, advisor code, or contact_id."""
    query = {}
    id = request.args.get('id', None)
    code = request.args.get('code', None)
    contact_id = request.args.get('contact_id', None)
    if id:
        query['id'] = id
    if code:
        query['code'] = code
    if contact_id:
        query['contact_id'] = contact_id
    return read_advisors(query=query)


@bp.route('/contact', methods=['POST'])
@format_response
def link_advisor_contact_route():
    """Link an advisor to a contact selected by an operator."""
    payload = request.get_json(force=True)
    return link_advisor_contact(
        advisor_id=payload.get('advisor_id'),
        contact_id=payload.get('contact_id'),
    )


@bp.route('/contact/create', methods=['POST'])
@format_response
def create_and_link_advisor_contact_route():
    """Create a contact and link it to an advisor atomically."""
    payload = request.get_json(force=True)
    return create_and_link_advisor_contact(
        advisor_id=payload.get('advisor_id'),
        contact=payload.get('contact'),
    )
