from flask import Blueprint, request

from src.components.clients.document_review_messages import read_document_review_messages, record_document_review_message
from src.utils.response import format_response

bp = Blueprint('document_review_messages', __name__)


@bp.route('/read', methods=['GET'])
@format_response
def read_route():
    query = {field: request.args[field] for field in ('account_id', 'contact_id') if request.args.get(field)}
    return read_document_review_messages(query, include_message=request.args.get('include_message') == 'true')


@bp.route('/record', methods=['POST'])
@format_response
def record_route():
    return record_document_review_message(request.get_json(silent=True))
