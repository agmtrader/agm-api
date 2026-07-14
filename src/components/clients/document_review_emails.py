from datetime import datetime

from src.components.tools.public.email import Email
from src.utils.connectors.supabase import db
from src.utils.exception import ServiceError, handle_exception

TABLE = 'document_review_email'
ALLOWED_DOCUMENT_KEYS = {'poi', 'poe', 'poa', 'sow'}
ALLOWED_RECIPIENT_SOURCES = {'contact', 'ibkr', 'testing'}


def _timestamp():
    return datetime.now().strftime('%Y%m%d%H%M%S')


def _validate_send_payload(payload):
    required_fields = ['account_id', 'contact_id', 'recipient_email', 'recipient_source', 'language', 'content']
    missing_fields = [field for field in required_fields if not payload.get(field)]
    if missing_fields:
        raise ServiceError(f"Missing required fields: {', '.join(missing_fields)}", status_code=400)

    if payload['recipient_source'] not in ALLOWED_RECIPIENT_SOURCES:
        raise ServiceError('recipient_source must be contact, ibkr, or testing', status_code=400)
    if payload['language'] not in {'en', 'es'}:
        raise ServiceError('language must be en or es', status_code=400)

    document_keys = payload.get('missing_document_keys')
    if not isinstance(document_keys, list) or not document_keys:
        raise ServiceError('missing_document_keys must be a non-empty array', status_code=400)

    normalized_keys = list(dict.fromkeys(str(key).strip().lower() for key in document_keys))
    if any(key not in ALLOWED_DOCUMENT_KEYS for key in normalized_keys):
        raise ServiceError('missing_document_keys contains an unsupported document key', status_code=400)
    if 'poi' in normalized_keys and 'poe' in normalized_keys:
        raise ServiceError('A review email cannot request both POI and POE', status_code=400)

    content = payload['content']
    if not isinstance(content, dict) or not content.get('missing_documents'):
        raise ServiceError('content.missing_documents is required', status_code=400)

    is_company_contact = bool(content.get('is_company_contact'))
    if is_company_contact and 'poi' in normalized_keys:
        raise ServiceError('Company review emails cannot request POI', status_code=400)
    if not is_company_contact and 'poe' in normalized_keys:
        raise ServiceError('Natural-person review emails cannot request POE', status_code=400)

    linked_contact = db.read(
        table='account_contact',
        query={
            'account_id': payload['account_id'],
            'contact_id': payload['contact_id'],
        },
    )
    if not linked_contact:
        raise ServiceError('The contact is not linked to the selected account', status_code=400)

    return normalized_keys


@handle_exception
def send_document_review_email(payload=None):
    if not isinstance(payload, dict):
        raise ServiceError('Request payload is required', status_code=400)

    normalized_keys = _validate_send_payload(payload)
    attempt_id = db.create(
        table=TABLE,
        data={
            'account_id': payload['account_id'],
            'contact_id': payload['contact_id'],
            'recipient_email': str(payload['recipient_email']).strip(),
            'recipient_source': payload['recipient_source'],
            'missing_document_keys': normalized_keys,
            'language': payload['language'],
            'status': 'pending',
            'provider': 'gmail',
        },
    )

    try:
        provider_message_id = Email.send_missing_documents_email(
            content=payload['content'],
            client_email=str(payload['recipient_email']).strip(),
            missing_type='multiple',
            lang=payload['language'],
            cc=payload.get('cc', ''),
        )
    except Exception as exc:
        try:
            db.update(
                table=TABLE,
                query={'id': attempt_id},
                data={
                    'status': 'failed',
                    'error_message': str(exc)[:2000],
                },
            )
        except Exception:
            pass
        raise

    sent_at = _timestamp()
    db.update(
        table=TABLE,
        query={'id': attempt_id},
        data={
            'status': 'sent',
            'provider_message_id': provider_message_id,
            'sent_at': sent_at,
            'error_message': None,
        },
    )
    return {
        'id': attempt_id,
        'status': 'sent',
        'provider': 'gmail',
        'provider_message_id': provider_message_id,
        'sent_at': sent_at,
    }


@handle_exception
def read_document_review_emails(query=None):
    return db.read(table=TABLE, query=query or {})
