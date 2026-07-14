from flask import Blueprint, request

from src.components.clients.document_review_emails import (
    read_document_review_emails,
    send_document_review_email,
)
from src.utils.response import format_response

bp = Blueprint('document_review_emails', __name__)


@bp.route('/read', methods=['GET'])
@format_response
def read_document_review_emails_route():
    """Read tracked document-review email attempts by account, contact, or status."""
    query = {}
    for field in ('id', 'account_id', 'contact_id', 'status'):
        value = request.args.get(field)
        if value:
            query[field] = value
    return read_document_review_emails(query=query)


@bp.route('/send', methods=['POST'])
@format_response
def send_document_review_email_route():
    """Send and persist a missing-documents email attempt for a review row."""
    return send_document_review_email(payload=request.get_json(force=True))
