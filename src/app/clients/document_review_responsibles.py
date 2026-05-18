from flask import Blueprint, request

from src.components.clients.document_review_responsibles import (
    create_document_review_responsible,
    read_document_review_responsibles,
    delete_document_review_responsible,
)
from src.utils.response import format_response

bp = Blueprint('document_review_responsibles', __name__)


@bp.route('/create', methods=['POST'])
@format_response
def create_document_review_responsible_route():
    payload = request.get_json(force=True)
    document_review_responsible = payload.get('document_review_responsible', None)
    return create_document_review_responsible(document_review_responsible=document_review_responsible)


@bp.route('/read', methods=['GET'])
@format_response
def read_document_review_responsibles_route():
    query = {}
    id = request.args.get('id', None)
    document_id = request.args.get('document_id', None)
    account_id = request.args.get('account_id', None)
    contact_id = request.args.get('contact_id', None)
    user_id = request.args.get('user_id', None)

    if id:
        query['id'] = id
    if document_id:
        query['document_id'] = document_id
    if account_id:
        query['account_id'] = account_id
    if contact_id:
        query['contact_id'] = contact_id
    if user_id:
        query['user_id'] = user_id

    return read_document_review_responsibles(query=query)


@bp.route('/delete', methods=['POST'])
@format_response
def delete_document_review_responsible_route():
    payload = request.get_json(force=True)
    query = payload.get('query', None)
    return delete_document_review_responsible(query=query)
