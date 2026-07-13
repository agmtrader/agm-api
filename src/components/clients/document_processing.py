import base64
from functools import lru_cache
from io import BytesIO
import os
import re
from typing import Optional

import easyocr
import numpy as np
from PIL import Image
from pypdf import PdfReader
import pypdfium2
import torch

from src.utils.connectors.supabase import db
from src.utils.exception import handle_exception
from src.utils.logger import logger

TABLE = 'document_processing'
TEXT_EXTRACTION_PROCESS_TYPE = 'text_extraction'
DIRECT_TEXT_EXTRACTION_PROVIDER = 'local_direct_parser'
OCR_TEXT_EXTRACTION_PROVIDER = 'local_easyocr'
OCR_LANGUAGE_MAP = {
    'en': 'en',
    'eng': 'en',
    'english': 'en',
    'es': 'es',
    'spa': 'es',
    'spanish': 'es',
}
DEFAULT_OCR_DEVICE = 'auto'
OCR_ROTATIONS = (0, 90, 180, 270)
OCR_MIN_QUALITY_SCORE = 80.0


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


def _get_ocr_language_codes(source_language: Optional[str]) -> str:
    normalized_language = _normalize_language(source_language)
    if not normalized_language:
        return 'es'

    parts = []
    for raw_part in normalized_language.replace(';', ',').replace('+', ',').split(','):
        part = raw_part.strip()
        if not part:
            continue
        mapped = OCR_LANGUAGE_MAP.get(part)
        if mapped and mapped not in parts:
            parts.append(mapped)

    return '+'.join(parts) if parts else 'es'


def _get_easyocr_languages(source_language: Optional[str]) -> list[str]:
    raw_languages = _get_ocr_language_codes(source_language).split('+')
    languages = []
    for language in raw_languages + ['en']:
        if language and language not in languages:
            languages.append(language)
    return languages or ['es', 'en']


def _resolve_easyocr_device() -> str:
    configured_device = str(
        os.getenv('EASYOCR_DEVICE', DEFAULT_OCR_DEVICE)
    ).strip().lower() or DEFAULT_OCR_DEVICE

    if configured_device == 'cpu':
        return 'cpu'

    if configured_device == 'cuda':
        if torch.cuda.is_available():
            return 'cuda'
        logger.warning('EASYOCR_DEVICE=cuda was requested but CUDA is unavailable. Falling back to CPU.')
        return 'cpu'

    if configured_device == 'mps':
        if torch.backends.mps.is_available():
            return 'mps'
        logger.warning('EASYOCR_DEVICE=mps was requested but MPS is unavailable. Falling back to CPU.')
        return 'cpu'

    if configured_device != DEFAULT_OCR_DEVICE:
        logger.warning(f'Unsupported EASYOCR_DEVICE value: {configured_device}. Falling back to auto detection.')

    if torch.cuda.is_available():
        return 'cuda'

    if torch.backends.mps.is_available():
        return 'mps'

    return 'cpu'


@lru_cache(maxsize=8)
def _get_easyocr_reader(languages_key: tuple[str, ...], device: str):
    logger.info(f'Initializing EasyOCR reader with device={device} languages={list(languages_key)}')
    return easyocr.Reader(list(languages_key), gpu=device)


def _score_ocr_text(text: str) -> float:
    normalized = str(text or '').strip()
    if not normalized:
        return 0.0

    letters = len(re.findall(r'[A-Za-zÀ-ÿ]', normalized))
    digits = len(re.findall(r'\d', normalized))
    weird_chars = len(re.findall(r'[^\w\sÀ-ÿ:;,\.\-\/()]', normalized))
    longer_words = re.findall(r'\b[A-Za-zÀ-ÿ]{3,}\b', normalized)
    isolated_digit_tokens = len(re.findall(r'\b\d\b', normalized))

    score = 0.0
    score += letters * 1.0
    score += len(longer_words) * 8.0
    score -= weird_chars * 6.0
    score -= isolated_digit_tokens * 2.0

    if digits > letters:
        score -= (digits - letters) * 0.5

    return score


def _extract_best_ocr_text_from_image(reader, image: Image.Image) -> tuple[str, float, int]:
    best_text = ''
    best_score = float('-inf')
    best_rotation = 0

    for rotation in OCR_ROTATIONS:
        rotated = image if rotation == 0 else image.rotate(rotation, expand=True)
        try:
            page_result = reader.readtext(np.array(rotated), detail=0, paragraph=True)
        finally:
            if rotation != 0:
                rotated.close()

        candidate_text = '\n'.join(
            str(chunk).strip() for chunk in page_result if str(chunk).strip()
        ).strip()
        candidate_score = _score_ocr_text(candidate_text)

        if candidate_score > best_score:
            best_text = candidate_text
            best_score = candidate_score
            best_rotation = rotation

    return best_text, best_score, best_rotation


def _extract_pdf_text_with_ocr(file_bytes: bytes, source_language: Optional[str]) -> str:
    reader_languages = _get_easyocr_languages(source_language)
    ocr_device = _resolve_easyocr_device()
    dpi = 300
    scale = dpi / 72

    try:
        pdf = pypdfium2.PdfDocument(BytesIO(file_bytes))
    except Exception as exc:
        raise ValueError(f'Failed to open PDF for OCR rendering: {exc}') from exc

    reader = _get_easyocr_reader(tuple(reader_languages), ocr_device)
    extracted_pages = []
    try:
        for page_index in range(len(pdf)):
            page = pdf[page_index]
            bitmap = page.render(scale=scale)
            image = bitmap.to_pil()
            try:
                page_text, page_score, page_rotation = _extract_best_ocr_text_from_image(reader, image)
            finally:
                image.close()
                page.close()

            if page_text:
                logger.info(
                    f'OCR page {page_index + 1}: selected rotation={page_rotation} score={page_score:.1f}'
                )
                extracted_pages.append(page_text)
    finally:
        pdf.close()

    extracted_text = '\n\n'.join(extracted_pages).strip()
    if extracted_text and _score_ocr_text(extracted_text) < OCR_MIN_QUALITY_SCORE:
        raise ValueError('PDF OCR produced low-quality text.')
    return extracted_text


def _extract_document_text(
    document_row: dict,
    source_language: Optional[str],
) -> tuple[str, str]:
    mime_type = str((document_row or {}).get('mime_type') or '').strip().lower()
    file_bytes = _decode_document_bytes(document_row)

    if mime_type == 'application/pdf':
        text = _extract_pdf_text(file_bytes)
        if text:
            return text, DIRECT_TEXT_EXTRACTION_PROVIDER

        logger.warning('PDF has no extractable text via pypdf. Falling back to local OCR.')
        ocr_text = _extract_pdf_text_with_ocr(file_bytes, source_language)
        if not ocr_text:
            raise ValueError('PDF OCR completed but produced no text.')
        return ocr_text, OCR_TEXT_EXTRACTION_PROVIDER

    if mime_type.startswith('text/'):
        text = _extract_plain_text(file_bytes)
        if not text:
            raise ValueError('Text document is empty after decoding.')
        return text, DIRECT_TEXT_EXTRACTION_PROVIDER

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
        output_text, provider = _extract_document_text(document_row, normalized_language)
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
            provider=DIRECT_TEXT_EXTRACTION_PROVIDER,
            error=str(exc),
        )
        return {'id': record_id, 'status': 'failed', 'error': str(exc)}
