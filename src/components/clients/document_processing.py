import base64
from io import BytesIO
from typing import Optional

from pypdf import PdfReader

from src.utils.connectors.supabase import db
from src.utils.exception import handle_exception
from src.utils.logger import logger

TABLE = 'document_processing'
TEXT_EXTRACTION_PROCESS_TYPE = 'text_extraction'
TEXT_EXTRACTION_PROVIDER = 'local_direct_parser'


def _normalize_language(document_language: Optional[str]) -> Optional[str]:
    normalized = str(document_language or '').strip().lower()
    return normalized or None


def _decode_document_bytes(document_row: dict) -> bytes:
    encoded_data = str((document_row or {}).get('data') or '').strip()
    if not encoded_data:
      raise ValueError('Document data is empty.')
    return base64.b64decode(encoded_data)


def _extract_pdf_text(file_bytes: bytes) -> str:
    reader = PdfReader(BytesIO(file_bytes))
    pages = []
    for page in reader.pages:
        page_text = page.extract_text() or ''
        if page_text.strip():
            pages.append(page_text.strip())
    return '\n\n'.join(pages).strip()


def _extract_plain_text(file_bytes: bytes) -> str:
    for encoding in ('utf-8', 'latin-1'):
        try:
            return file_bytes.decode(encoding).strip()
        except UnicodeDecodeError:
            continue
    raise ValueError('Unable to decode document as plain text.')


def _extract_document_text(document_row: dict) -> tuple[str, str]:
    mime_type = str((document_row or {}).get('mime_type') or '').strip().lower()
    file_bytes = _decode_document_bytes(document_row)

    if mime_type == 'application/pdf':
        text = _extract_pdf_text(file_bytes)
        if not text:
            raise ValueError('PDF has no extractable text. OCR is required.')
        return text, TEXT_EXTRACTION_PROVIDER

    if mime_type.startswith('text/'):
        text = _extract_plain_text(file_bytes)
        if not text:
            raise ValueError('Text document is empty after decoding.')
        return text, TEXT_EXTRACTION_PROVIDER

    raise ValueError(f'Unsupported mime type for direct extraction: {mime_type or "unknown"}')


def _upsert_document_processing_record(
    document_id: str,
    source_language: Optional[str],
    status: str,
    output_text: Optional[str],
    provider: Optional[str],
    error: Optional[str],
):
    existing_records = db.read(
        table=TABLE,
        query={'document_id': document_id, 'process_type': TEXT_EXTRACTION_PROCESS_TYPE},
    ) or []

    payload = {
        'document_id': document_id,
        'process_type': TEXT_EXTRACTION_PROCESS_TYPE,
        'status': status,
        'source_language': source_language,
        'output_text': output_text,
        'provider': provider,
        'error': error,
    }

    if existing_records:
        return db.update(
            table=TABLE,
            query={'document_id': document_id, 'process_type': TEXT_EXTRACTION_PROCESS_TYPE},
            data=payload,
        )

    return db.create(table=TABLE, data=payload)


@handle_exception
def process_document_text_extraction(document_id: str, source_language: Optional[str] = None):
    if not document_id:
        raise ValueError('document_id is required')

    normalized_language = _normalize_language(source_language)
    document_rows = db.read(table='document', query={'id': document_id}) or []
    if not document_rows:
        raise ValueError(f'Document not found: {document_id}')

    document_row = document_rows[0]

    try:
        output_text, provider = _extract_document_text(document_row)
        record_id = _upsert_document_processing_record(
            document_id=document_id,
            source_language=normalized_language,
            status='completed',
            output_text=output_text,
            provider=provider,
            error=None,
        )
        return {'id': record_id, 'status': 'completed'}
    except Exception as exc:
        logger.warning(f'Failed text extraction for document {document_id}: {exc}')
        record_id = _upsert_document_processing_record(
            document_id=document_id,
            source_language=normalized_language,
            status='failed',
            output_text=None,
            provider=TEXT_EXTRACTION_PROVIDER,
            error=str(exc),
        )
        return {'id': record_id, 'status': 'failed', 'error': str(exc)}

