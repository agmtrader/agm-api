"""Manual WhatsApp send evidence. This service never sends a message."""
from datetime import datetime, timezone
import re
from uuid import UUID

from flask import g
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from src.utils.connectors.supabase import db
from src.utils.exception import ServiceError, handle_exception

TABLE = 'document_review_message'


def _uuid(value, field):
    try:
        return str(UUID(str(value)))
    except (ValueError, TypeError, AttributeError):
        raise ServiceError(f'{field} must be a UUID', status_code=400)


def _text(payload, field, maximum):
    value = payload.get(field)
    if not isinstance(value, str) or not value.strip() or len(value) > maximum:
        raise ServiceError(f'{field} is required (maximum {maximum} characters)', status_code=400)
    return value.strip()


@handle_exception
def record_document_review_message(payload=None):
    if not isinstance(payload, dict):
        raise ServiceError('Request payload is required', status_code=400)
    data = {field: _uuid(payload.get(field), field) for field in (
        'id', 'account_id', 'contact_id', 'recipient_contact_id',
    )}
    user = getattr(g, 'current_user', None)
    if not user or not user.get('id'):
        raise ServiceError('Authentication required', status_code=401)
    data['recorded_by'] = _uuid(user['id'], 'recorded_by')
    data['recorded_by_name'] = str(user.get('email') or user['id'])
    for field, choices in (
        ('template', {'initial', 'follow_up'}),
        ('language', {'en', 'es'}),
        ('holder_scenario', {'own', 'secondary', 'company'}),
    ):
        value = payload.get(field)
        if not isinstance(value, str) or value not in choices:
            raise ServiceError(f'Invalid {field}', status_code=400)
        data[field] = value
    keys = payload.get('missing_document_keys')
    if (not isinstance(keys, list) or not keys or
            any(not isinstance(key, str) or key not in {'poi', 'poe', 'poa', 'sow'} for key in keys) or
            ('poi' in keys and 'poe' in keys)):
        raise ServiceError('Invalid missing_document_keys', status_code=400)
    data['missing_document_keys'] = list(dict.fromkeys(keys))
    data['recipient_name'] = _text(payload, 'recipient_name', 300)
    data['recipient_phone'] = _text(payload, 'recipient_phone', 80)
    if not re.fullmatch(r'\+?[\d\s().-]+', data['recipient_phone']) or not 7 <= len(re.sub(r'\D', '', data['recipient_phone'])) <= 15:
        raise ServiceError('Enter a valid recipient phone number, including country code', status_code=400)
    data['message_text'] = _text(payload, 'message_text', 12000)
    # A copied snapshot must survive a retry unchanged. Send time and reviewer
    # attribution are assigned by the server on the first successful insert.
    data['channel'] = 'whatsapp'
    data['status'] = 'sent_manual'

    @db.with_session
    def save(session):
        links = db.metadata.tables['account_contact']
        for field in ('contact_id', 'recipient_contact_id'):
            linked = session.execute(select(links.c.role).where(
                links.c.account_id == UUID(data['account_id']),
                links.c.contact_id == UUID(data[field]),
            )).all()
            if not linked:
                raise ServiceError(f'{field} is not linked to this account', status_code=400)
            if any('trusted' in str(row.role or '').lower() for row in linked):
                raise ServiceError('Trusted contacts cannot be used for document outreach', status_code=400)
        contacts = db.metadata.tables['contact']
        owner_type = session.execute(select(contacts.c.type).where(
            contacts.c.id == UUID(data['contact_id'])
        )).scalar_one()
        company = str(owner_type or '').strip().lower() == 'company'
        if (company and 'poi' in keys) or (not company and 'poe' in keys):
            raise ServiceError('Document keys do not match the document owner type', status_code=400)
        if company != (data['holder_scenario'] == 'company'):
            raise ServiceError('Holder scenario does not match the document owner type', status_code=400)
        if data['holder_scenario'] == 'secondary' and data['recipient_contact_id'] == data['contact_id']:
            raise ServiceError('Select the recipient helping the secondary holder', status_code=400)
        table = db.metadata.tables[TABLE]
        now = datetime.now(timezone.utc).isoformat(timespec='seconds')
        values = {**data, 'created': now, 'updated': now, 'sent_at': now}
        for field in ('id', 'account_id', 'contact_id', 'recipient_contact_id', 'recorded_by'):
            values[field] = UUID(values[field])
        session.execute(insert(table).values(**values).on_conflict_do_nothing(index_elements=['id']))
        saved = dict(session.execute(select(table).where(table.c.id == UUID(data['id']))).mappings().one())
        if any(str(saved[field]) != str(value) for field, value in data.items()):
            raise ServiceError('This send identifier was already used for a different record', status_code=409)
        return {key: str(value) if isinstance(value, UUID) else value for key, value in saved.items()}

    return save()


@handle_exception
def read_document_review_messages(query=None, include_message=False):
    query = query or {}
    normalized = {field: _uuid(value, field) for field, value in query.items()}
    if include_message and not (normalized.get('account_id') and normalized.get('contact_id')):
        raise ServiceError('Message text requires an account_id and contact_id', status_code=400)
    return db.read(TABLE, {field: UUID(value) for field, value in normalized.items()}, exclude_columns=[] if include_message else ['message_text'],
                   order_by=('sent_at', 'id'))
