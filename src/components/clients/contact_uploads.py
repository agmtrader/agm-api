"""Atomic content reuse and upload-triggered OCR; no provider call inside a DB transaction."""
import base64
import binascii
import hashlib
import uuid
from datetime import datetime

from sqlalchemy import func, select, text

from src.utils.exception import ServiceError
from src.utils.logger import logger

GOOGLE_OCR_PROVIDER = 'google_document_ai_enterprise_ocr'
PROCESS_TYPE = 'text_extraction'


def save_contact_upload(db, document, link):
    """All upload writers serialize by content; commit the file/link/OCR claim together."""
    try:
        raw = base64.b64decode(document.get('data') or '', validate=True)
    except (ValueError, TypeError, binascii.Error):
        raise ServiceError('Document data must be valid base64', 422)
    if not raw:
        raise ServiceError('Document data is empty', 422)
    checksum = hashlib.sha1(raw).hexdigest()
    supplied_checksum = str(document.get('sha1_checksum') or '').strip().lower()
    if supplied_checksum and supplied_checksum != checksum:
        raise ServiceError('Document checksum does not match its contents', 422)
    try:
        contact_id = uuid.UUID(str(link.get('contact_id')))
        account_id = uuid.UUID(str(link['account_id'])) if link.get('account_id') else None
    except (ValueError, TypeError, AttributeError):
        raise ServiceError('A valid contact_id and optional account_id are required', 422)
    if not str(document.get('file_name') or '').strip() or not str(document.get('mime_type') or '').strip():
        raise ServiceError('Document filename and MIME type are required', 422)
    documents = db.metadata.tables['document']
    links = db.metadata.tables['contact_document']
    processing = db.metadata.tables['document_processing']
    stamp = datetime.now().strftime('%Y%m%d%H%M%S')
    # Signed bigint from a separate namespace; the lock is released on commit/rollback.
    lock_key = int.from_bytes(hashlib.sha256(('contact-upload:' + checksum).encode()).digest()[:8], 'big', signed=True)
    with db.engine.begin() as connection:
        connection.execute(text('SELECT pg_advisory_xact_lock(:key)'), {'key': lock_key})
        candidates = connection.execute(select(documents).where(
            func.lower(func.trim(documents.c.sha1_checksum)) == checksum
        ).order_by(documents.c.created, documents.c.id)).mappings().all()
        existing = candidates[0] if candidates else None
        if existing:
            # Never use a client-supplied hash alone to authorize canonical-content reuse.
            if base64.b64decode(existing['data'], validate=True) != raw:
                raise ServiceError('Stored document checksum conflicts with uploaded contents', 409)
            document_id = existing['id']
        else:
            document_id = uuid.uuid4()
            connection.execute(documents.insert().values(
                id=document_id, created=stamp, updated=stamp,
                file_name=document['file_name'], file_length=str(len(raw)),
                sha1_checksum=checksum, mime_type=document['mime_type'],
                data=base64.b64encode(raw).decode('ascii'),
            ))
        # A contact gets one link per file across all account contexts; preserve its metadata.
        existing_link = connection.execute(select(links.c.id).where(
            links.c.document_id == document_id,
            links.c.contact_id == contact_id,
        ).order_by(links.c.created, links.c.id).limit(1)).scalar_one_or_none()
        link_id = existing_link or uuid.uuid4()
        if not existing_link:
            normalized_link = db._dates_to_timestamp(dict(link))
            normalized_link.update(contact_id=contact_id, account_id=account_id, document_id=document_id)
            connection.execute(links.insert().values(
                id=link_id, created=stamp, updated=stamp, **normalized_link,
            ))
        records = connection.execute(select(processing).where(
            processing.c.document_id == document_id,
            processing.c.process_type == PROCESS_TYPE,
        ).order_by(processing.c.created, processing.c.id)).mappings().all()
        completed = any(row['status'] == 'completed' and str(row['output_text'] or '').strip() for row in records)
        # A prior failed/pending attempt belongs to recovery, not upload retries.
        claimed = not records
        if claimed:
            connection.execute(processing.insert().values(
                id=uuid.uuid4(), created=stamp, updated=stamp,
                document_id=document_id, process_type=PROCESS_TYPE,
                status='pending', source_language=link.get('document_language') or None,
                output_text=None, provider=GOOGLE_OCR_PROVIDER, error=None,
            ))
    return {'id': str(link_id), 'document_id': str(document_id),
            'duplicate': existing is not None, 'link_reused': existing_link is not None}, claimed and not completed


def attempt_upload_ocr(db, document_id, source_language):
    """OCR is best effort after durable upload success; pending rows survive worker loss."""
    try:
        from src.components.clients.document_processing import process_document_text_extraction
        process_document_text_extraction(
            document_id=document_id, source_language=source_language,
            provider=GOOGLE_OCR_PROVIDER,
        )
    except Exception as exc:
        # Includes module/config initialization and processing persistence failures.
        # A saved file must never turn into a failed client upload due to OCR.
        logger.warning(f'Upload OCR deferred document_id={document_id} exception_type={type(exc).__name__}')
        try:
            db.update(table='document_processing',
                      query={'document_id': document_id, 'process_type': PROCESS_TYPE},
                      data={'status': 'failed', 'output_text': None, 'provider': GOOGLE_OCR_PROVIDER,
                            'error': f'Upload OCR failed: {type(exc).__name__}'})
        except Exception as persistence_exc:
            logger.warning(f'Upload OCR status persistence deferred document_id={document_id} exception_type={type(persistence_exc).__name__}')


def mutate_contact_upload(db, document_id, link_id=None, *, data=None, delete=False):
    """Edit/unlink the selected association; shared raw content belongs to all its links."""
    try:
        document_id = uuid.UUID(str(document_id))
        link_id = uuid.UUID(str(link_id)) if link_id else None
    except (ValueError, TypeError, AttributeError):
        raise ServiceError('A valid document and contact-document ID are required', 422)
    documents = db.metadata.tables['document']
    links = db.metadata.tables['contact_document']
    with db.engine.begin() as connection:
        checksum = connection.execute(select(documents.c.sha1_checksum).where(documents.c.id == document_id)).scalar_one_or_none()
        if checksum is None:
            raise ServiceError('Document not found', 404)
        lock_key = int.from_bytes(hashlib.sha256(('contact-upload:' + checksum.strip().lower()).encode()).digest()[:8], 'big', signed=True)
        connection.execute(text('SELECT pg_advisory_xact_lock(:key)'), {'key': lock_key})
        rows = connection.execute(select(links.c.id).where(links.c.document_id == document_id)).scalars().all()
        if link_id:
            if link_id not in rows:
                raise ServiceError('Contact-document link not found', 404)
        elif len(rows) == 1:
            link_id = rows[0]
        elif not rows:
            raise ServiceError('Contact-document link not found', 404)
        else:
            raise ServiceError('Select the contact-document link for this shared document', 409)
        if delete:
            connection.execute(links.delete().where(links.c.id == link_id))
            if len(rows) == 1:
                connection.execute(documents.delete().where(documents.c.id == document_id))
            return {'status': 'success'}
        connection.execute(links.update().where(links.c.id == link_id).values(
            **db._dates_to_timestamp(dict(data or {})), updated=datetime.now().strftime('%Y%m%d%H%M%S'),
        ))
        return str(link_id)
