"""Validate statement contents independently of IBKR's HTTP status/content type."""
import base64
import binascii

from src.utils.exception import ServiceError


def statement_document(response):
    """Return the same PDF envelope for raw PDFs and JSON/base64 documents.

    IBKR also sends HTTP 200 text/plain containing JSON with a business-error
    detail. That message is not a document, regardless of the HTTP status.
    """
    content = response.content
    if content.startswith(b'%PDF-'):
        encoded = base64.b64encode(content).decode('ascii')
    else:
        try:
            payload = response.json()
        except ValueError:
            payload = None
        data = payload.get('data') if isinstance(payload, dict) else None
        encoded = data.get('value') if isinstance(data, dict) else None
        if not isinstance(encoded, str) or not encoded:
            detail = payload.get('detail') if isinstance(payload, dict) else None
            if isinstance(detail, str) and detail.strip():
                raise ServiceError(detail.strip()[:2000], 409, code='ibkr_statement_unavailable')
            raise ServiceError('IBKR did not return a valid PDF statement.', 502, code='ibkr_statement_invalid')
        try:
            content = base64.b64decode(encoded, validate=True)
        except (ValueError, binascii.Error):
            content = b''
        if not content.startswith(b'%PDF-'):
            raise ServiceError('IBKR did not return a valid PDF statement.', 502, code='ibkr_statement_invalid')

    return {'data': {'dataType': 'String', 'mimeType': 'application/pdf',
                     'encoding': 'base64', 'value': encoded}}
