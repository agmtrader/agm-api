"""Staff review of stored documents and their contact associations."""

from datetime import date, datetime, timedelta, timezone
import hashlib
from uuid import UUID, uuid4

from sqlalchemy import and_, func, or_, select, text

from src.utils.exception import ServiceError
from src.utils.logger import logger


QUEUES = {'metadata', 'unlinked', 'checksums'}
PAGE_SIZE = 25
FIELDS = {'category', 'type', 'document_language', 'issued_date', 'expiry_date', 'comment'}


def read_review_queue(db, queue, page):
    if queue not in QUEUES or not 1 <= page <= 10000:
        raise ServiceError('Invalid document review queue or page.', 400)
    documents = db.metadata.tables['document']
    links = db.metadata.tables['contact_document']
    contacts = db.metadata.tables['contact']
    source = documents.outerjoin(links, links.c.document_id == documents.c.id).outerjoin(
        contacts, links.c.contact_id == contacts.c.id
    )
    if queue == 'unlinked':
        condition = links.c.id.is_(None)
    elif queue == 'checksums':
        checksum = func.lower(func.trim(documents.c.sha1_checksum))
        groups = select(checksum).where(checksum != '').group_by(checksum).having(func.count() > 1)
        condition = checksum.in_(groups)
    else:
        category = func.lower(func.trim(links.c.category))
        missing_type = func.nullif(func.trim(links.c.type), '').is_(None)
        missing_issue = func.nullif(func.trim(links.c.issued_date), '').is_(None)
        missing_expiry = func.nullif(func.trim(links.c.expiry_date), '').is_(None)
        condition = or_(
            and_(category == 'proof of identity', or_(missing_type, missing_expiry)),
            and_(category == 'proof of address', or_(missing_type, missing_issue)),
            and_(category == 'source of wealth', missing_type),
        )
    query = select(
        documents.c.id.label('document_id'), documents.c.file_name, documents.c.mime_type,
        links.c.id.label('link_id'), links.c.contact_id, contacts.c.name.label('contact_name'),
        links.c.account_id, links.c.updated, *(links.c[field] for field in sorted(FIELDS)),
    ).select_from(source).where(condition)
    order = [documents.c.created.desc(), documents.c.id, links.c.id]
    if queue == 'checksums':
        query = query.add_columns(checksum.label('checksum'))
        order.insert(0, checksum)
    with db.engine.connect() as connection:
        total = connection.execute(select(func.count()).select_from(source).where(condition)).scalar_one()
        rows = connection.execute(query.order_by(*order).offset((page - 1) * PAGE_SIZE).limit(PAGE_SIZE))
        return {'rows': [dict(row) for row in rows.mappings()], 'total': total, 'page_size': PAGE_SIZE}


def read_review_file(db, document_id):
    documents = db.metadata.tables['document']
    with db.engine.connect() as connection:
        row = connection.execute(select(
            documents.c.id, documents.c.file_name, documents.c.mime_type, documents.c.data,
        ).where(documents.c.id == document_id)).mappings().one_or_none()
    if row is None:
        raise ServiceError('Document not found.', 404)
    return dict(row)


def _review_values(payload):
    fields = payload.get('metadata')
    if not isinstance(fields, dict) or not fields.keys() <= FIELDS:
        raise ServiceError('Provide supported document metadata fields.', 422)
    values = {}
    for field, value in fields.items():
        if value is not None and (not isinstance(value, str) or '\x00' in value or len(value) > (2000 if field == 'comment' else 200)):
            raise ServiceError('Invalid document metadata value.', 422)
        value = (value or '').strip() or None
        if field in {'issued_date', 'expiry_date'} and value:
            try:
                value = date.fromisoformat(value).strftime('%Y%m%d000000')
            except ValueError:
                raise ServiceError('Enter a valid issue or expiry date.', 422) from None
        values[field] = value
    return values


def save_review(db, document_id, payload, reviewer_id):
    if not isinstance(payload, dict) or set(payload) != {'link_id', 'contact_id', 'expected_updated', 'metadata'}:
        raise ServiceError('Provide the document link, contact, version and metadata.', 422)
    try:
        contact_id = UUID(payload['contact_id'])
        link_id = UUID(payload['link_id']) if payload['link_id'] is not None else None
    except (TypeError, ValueError, AttributeError):
        raise ServiceError('Select a valid contact and document link.', 422) from None
    if link_id and not isinstance(payload['expected_updated'], str):
        raise ServiceError('Reload the document before saving.', 422)
    values = _review_values(payload)
    documents = db.metadata.tables['document']
    links = db.metadata.tables['contact_document']
    contacts = db.metadata.tables['contact']
    account_contacts = db.metadata.tables['account_contact']
    with db.engine.begin() as connection:
        document = connection.execute(select(documents.c.sha1_checksum).where(
            documents.c.id == document_id
        )).one_or_none()
        if document is None:
            raise ServiceError('Document not found.', 404)
        checksum = (document.sha1_checksum or '').strip().lower()
        lock_key = int.from_bytes(hashlib.sha256(('contact-upload:' + checksum).encode()).digest()[:8], 'big', signed=True)
        connection.execute(text('SELECT pg_advisory_xact_lock(:key)'), {'key': lock_key})
        if connection.execute(select(documents.c.id).where(documents.c.id == document_id).with_for_update()).scalar_one_or_none() is None:
            raise ServiceError('Document not found.', 404)
        rows = connection.execute(select(links).where(links.c.document_id == document_id).with_for_update()).mappings().all()
        current = next((row for row in rows if row['id'] == link_id), None)
        if (link_id and (not current or current['updated'] != payload['expected_updated'])) or (not link_id and rows):
            raise ServiceError('The document links changed. Reload the queue and reopen the document.', 409)
        if connection.execute(select(contacts.c.id).where(contacts.c.id == contact_id).with_for_update(read=True, key_share=True)).scalar_one_or_none() is None:
            raise ServiceError('The selected contact no longer exists.', 422)
        reassigned = current is not None and current['contact_id'] != contact_id
        if current and not reassigned and not values:
            raise ServiceError('No document changes were provided.', 422)
        if reassigned and any(row['id'] != link_id and row['contact_id'] == contact_id for row in rows):
            raise ServiceError('The document already has a link to that contact.', 409)
        if reassigned and current['account_id']:
            linked = connection.execute(select(account_contacts.c.id).where(
                account_contacts.c.account_id == current['account_id'],
                account_contacts.c.contact_id == contact_id,
            ).limit(1).with_for_update(read=True)).scalar_one_or_none()
            if linked is None:
                raise ServiceError('Choose a contact linked to this document’s account.', 422)
        category = values.get('category', current['category'] if current else None)
        if not category and not current:
            raise ServiceError('Choose a category for the new contact link.', 422)
        if current and (current['category'] or '').strip().lower() == 'proof of existence' and (category or '').lower() != 'proof of existence':
            raise ServiceError('Proof of Existence documents must retain their category.', 422)
        now = datetime.now(timezone.utc)
        stamp = now.strftime('%Y%m%d%H%M%S')
        if current and stamp <= current['updated']:
            try:
                stamp = (datetime.strptime(current['updated'], '%Y%m%d%H%M%S') + timedelta(seconds=1)).strftime('%Y%m%d%H%M%S')
            except ValueError:
                pass
        values.update(contact_id=contact_id, updated=stamp)
        if current:
            connection.execute(links.update().where(links.c.id == link_id).values(**values))
        else:
            link_id = uuid4()
            connection.execute(links.insert().values(id=link_id, document_id=document_id, created=stamp, **values))
    logger.info(f'Document review saved reviewer={reviewer_id} document={document_id} link={link_id} '
                f'action={"reassigned" if reassigned else "updated" if current else "linked"}')
    return {'link_id': str(link_id)}
