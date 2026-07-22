import base64
from dataclasses import dataclass, field
from functools import lru_cache
import hashlib
from io import BytesIO
import json
import os
from pathlib import Path
import threading
from typing import Optional, Protocol, Sequence
import unicodedata

import cv2
import easyocr
import numpy as np
from PIL import Image, ImageOps
import pypdfium2
import torch
from easyocr.utils import reformat_input

from src.utils.exception import handle_exception
from src.utils.logger import logger

TABLE = 'document_processing'
TEXT_EXTRACTION_PROCESS_TYPE = 'text_extraction'
DIRECT_TEXT_EXTRACTION_PROVIDER = 'local_direct_parser'
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
OCR_RENDER_DPI = 300
OCR_MAX_RENDER_DIMENSION = 3500
OCR_MIN_LONG_EDGE = 1800
OCR_MAX_FILE_BYTES = 50 * 1024 * 1024
OCR_MAX_PAGES = 100
OCR_MAX_IMAGE_PIXELS = 50_000_000
OCR_ENHANCEMENT_RETRY_CONFIDENCE = 0.65
OCR_LOW_CONFIDENCE_REGION_THRESHOLD = 0.50
OCR_HIGH_QUALITY_SCORE = 80.0
OCR_REVIEW_QUALITY_SCORE = 50.0
POSITIONED_OCR_DPI = 200
EASYOCR_PROVIDER = 'local_easyocr_positioned'
GOOGLE_DOCUMENT_AI_PROVIDER = 'google_document_ai_enterprise_ocr'
POSITIONED_OCR_PROVIDER = EASYOCR_PROVIDER
DEFAULT_OCR_PROVIDER = GOOGLE_DOCUMENT_AI_PROVIDER
OCR_PIPELINE_VERSION = 'v6'
EASYOCR_VERSION = str(getattr(easyocr, '__version__', 'unknown'))
OCR_REVIEW_PROVIDER_QUALITY_SCORE = 0.65
OCR_LOW_PROVIDER_QUALITY_SCORE = 0.40
OCR_REVIEW_DEFECT_CONFIDENCE = 0.80
OCR_LOW_DEFECT_CONFIDENCE = 0.85
OCR_SEVERE_READABILITY_DEFECTS = frozenset(
    {
        'quality/defect_blurry',
        'quality/defect_dark',
        'quality/defect_faint',
        'quality/defect_noisy',
        'quality/defect_text_too_small',
    }
)
OCR_REVIEW_ONLY_DEFECTS = frozenset(
    {
        'quality/defect_glare',
        'quality/defect_text_cutoff',
    }
)
PROJECT_DIRECTORY = Path(__file__).resolve().parents[3]
OCR_DATA_DIRECTORY = Path(
    os.getenv('OCR_DATA_DIRECTORY', PROJECT_DIRECTORY / '.cache' / 'ocr')
).expanduser()
POSITIONED_OCR_MODEL_DIRECTORY = Path(
    os.getenv('EASYOCR_MODEL_DIRECTORY', OCR_DATA_DIRECTORY / 'models')
).expanduser()
POSITIONED_OCR_CACHE_DIRECTORY = Path(
    os.getenv('OCR_CACHE_DIRECTORY', OCR_DATA_DIRECTORY / 'results')
).expanduser()
POSITIONED_OCR_CACHE_VERSION = OCR_PIPELINE_VERSION
SUPPORTED_RASTER_MIME_TYPES = {
    'image/bmp',
    'image/gif',
    'image/jpeg',
    'image/png',
    'image/tiff',
    'image/webp',
}


@dataclass(slots=True)
class DocumentOCRRegion:
    """One positioned OCR paragraph and its source-page evidence."""

    polygon: list[list[float]]
    text: str
    confidence: float | None

    def as_dict(self) -> dict:
        return {
            'polygon': self.polygon,
            'text': self.text,
            'confidence': self.confidence,
        }


@dataclass(slots=True)
class DocumentOCRPage:
    """Positioned OCR output for one rendered document page."""

    page_number: int
    regions: list[DocumentOCRRegion]
    width: float
    height: float
    render_scale: float
    rotation: int = 0
    source_text: str | None = None
    lines: list[DocumentOCRRegion] = field(default_factory=list)
    words: list[DocumentOCRRegion] = field(default_factory=list)
    symbols: list[DocumentOCRRegion] = field(default_factory=list)
    image_quality_score: float | None = None
    quality_defects: list[dict] = field(default_factory=list)

    @property
    def text(self) -> str:
        if self.source_text:
            return self.source_text.strip()
        return '\n'.join(region.text for region in self.regions if region.text).strip()

    def as_dict(self) -> dict:
        return {
            'page_number': self.page_number,
            'width': self.width,
            'height': self.height,
            'render_scale': self.render_scale,
            'rotation': self.rotation,
            'text': self.text,
            'regions': [region.as_dict() for region in self.regions],
            'lines': [line.as_dict() for line in self.lines],
            'words': [word.as_dict() for word in self.words],
            'symbols': [symbol.as_dict() for symbol in self.symbols],
            'image_quality_score': self.image_quality_score,
            'quality_defects': self.quality_defects,
        }


@dataclass(slots=True)
class OCRQualityAssessment:
    """Document-neutral OCR quality signals used for review routing."""

    score: float
    status: str
    average_confidence: float | None
    character_count: int
    page_count: int
    region_count: int
    empty_page_count: int
    low_confidence_region_count: int
    printable_ratio: float
    suspicious_character_ratio: float
    reasons: list[str] = field(default_factory=list)
    provider_quality_score: float | None = None
    quality_defects: list[dict] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {
            'score': round(self.score, 1),
            'status': self.status,
            'average_confidence': (
                round(self.average_confidence, 4)
                if self.average_confidence is not None
                else None
            ),
            'character_count': self.character_count,
            'page_count': self.page_count,
            'region_count': self.region_count,
            'empty_page_count': self.empty_page_count,
            'low_confidence_region_count': self.low_confidence_region_count,
            'printable_ratio': round(self.printable_ratio, 4),
            'suspicious_character_ratio': round(
                self.suspicious_character_ratio,
                4,
            ),
            'reasons': self.reasons,
            'provider_quality_score': (
                round(self.provider_quality_score, 4)
                if self.provider_quality_score is not None
                else None
            ),
            'quality_defects': self.quality_defects,
        }


@dataclass(slots=True)
class DocumentOCRResult:
    """Canonical OCR output shared by every downstream document workflow."""

    pages: list[DocumentOCRPage]
    provider: str = POSITIONED_OCR_PROVIDER
    model_version: str = EASYOCR_VERSION
    pipeline_version: str = OCR_PIPELINE_VERSION
    device: str | None = None
    languages: list[str] = field(default_factory=list)
    source_hash: str | None = None
    quality: OCRQualityAssessment | None = None

    @property
    def text(self) -> str:
        return '\n\n'.join(page.text for page in self.pages if page.text).strip()

    def as_dict(self) -> dict:
        return {
            'provider': self.provider,
            'model_version': self.model_version,
            'pipeline_version': self.pipeline_version,
            'device': self.device,
            'languages': self.languages,
            'source_hash': self.source_hash,
            'text': self.text,
            'quality': self.quality.as_dict() if self.quality else None,
            'pages': [page.as_dict() for page in self.pages],
        }


@dataclass(slots=True)
class DocumentOCRRequest:
    """Provider-neutral OCR request assembled by the shared public boundary."""

    file_bytes: bytes
    source_language: str | None
    mime_type: str
    languages: list[str]
    ocr_device: str | None
    render_dpi: int
    rotations: tuple[int, ...]
    max_render_dimension: int | None
    enhance_low_quality: bool
    use_cache: bool
    max_file_bytes: int
    max_pages: int
    max_image_pixels: int
    page_numbers: list[int] | None


class DocumentOCRProvider(Protocol):
    """Contract implemented by every OCR backend."""

    name: str

    def validate_configuration(self) -> None:
        ...

    def extract(self, request: DocumentOCRRequest) -> DocumentOCRResult:
        ...


def _get_db():
    """Load the database connector only for database-backed operations."""

    from src.utils.connectors.supabase import db

    return db


def _normalize_language(document_language: Optional[str]) -> Optional[str]:
    normalized = str(document_language or '').strip().lower()
    return normalized or None


def _decode_document_bytes(document_row: dict) -> bytes:
    encoded_data = str((document_row or {}).get('data') or '').strip()
    if not encoded_data:
      raise ValueError('Document data is empty.')
    return base64.b64decode(encoded_data)


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
        mapped = OCR_LANGUAGE_MAP.get(part, part.split('-', 1)[0])
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


def _mps_is_available() -> bool:
    return bool(
        hasattr(torch.backends, 'mps')
        and torch.backends.mps.is_available()
    )


def _resolve_easyocr_device(requested_device: Optional[str] = None) -> str:
    configured_device = str(
        requested_device
        if requested_device is not None
        else os.getenv('EASYOCR_DEVICE', DEFAULT_OCR_DEVICE)
    ).strip().lower() or DEFAULT_OCR_DEVICE

    if configured_device == 'cpu':
        return 'cpu'

    if configured_device == 'cuda':
        if torch.cuda.is_available():
            return 'cuda'
        logger.warning('EASYOCR_DEVICE=cuda was requested but CUDA is unavailable. Falling back to CPU.')
        return 'cpu'

    if configured_device == 'mps':
        if _mps_is_available():
            return 'mps'
        logger.warning('EASYOCR_DEVICE=mps was requested but MPS is unavailable. Falling back to CPU.')
        return 'cpu'

    if configured_device != DEFAULT_OCR_DEVICE:
        logger.warning(f'Unsupported EASYOCR_DEVICE value: {configured_device}. Falling back to auto detection.')

    if torch.cuda.is_available():
        return 'cuda'

    if _mps_is_available():
        return 'mps'

    return 'cpu'


def _active_reader_device(reader, fallback: str) -> str:
    return str(getattr(reader, 'device', fallback)) if reader is not None else fallback


def _create_easyocr_reader(languages_key: tuple[str, ...], device: str):
    return easyocr.Reader(
        list(languages_key),
        gpu=device,
        model_storage_directory=str(POSITIONED_OCR_MODEL_DIRECTORY),
        user_network_directory=str(POSITIONED_OCR_MODEL_DIRECTORY / 'user_network'),
        verbose=False,
    )


@lru_cache(maxsize=8)
def _get_easyocr_reader(languages_key: tuple[str, ...], device: str):
    logger.info(
        f'Initializing EasyOCR reader with device={device} '
        f'languages={list(languages_key)}'
    )
    try:
        return _create_easyocr_reader(languages_key, device)
    except Exception as exc:
        if device == 'cpu':
            raise
        logger.warning(
            f'EasyOCR initialization failed on device={device}: {exc}. '
            'Falling back to CPU.'
        )
        return _create_easyocr_reader(languages_key, 'cpu')


def assess_ocr_text(
    text: str,
    average_confidence: float | None = None,
    *,
    page_count: int = 1,
    region_count: int = 0,
    empty_page_count: int = 0,
    low_confidence_region_count: int = 0,
) -> OCRQualityAssessment:
    """Assess OCR legibility without assuming a document type or language."""

    normalized = str(text or '').strip()
    normalized_page_count = max(1, int(page_count))
    normalized_region_count = max(0, int(region_count))
    normalized_empty_page_count = min(
        normalized_page_count,
        max(0, int(empty_page_count)),
    )
    normalized_low_confidence_count = min(
        normalized_region_count,
        max(0, int(low_confidence_region_count)),
    )
    if not normalized:
        return OCRQualityAssessment(
            score=0.0,
            status='low',
            average_confidence=average_confidence,
            character_count=0,
            page_count=normalized_page_count,
            region_count=normalized_region_count,
            empty_page_count=normalized_page_count,
            low_confidence_region_count=normalized_low_confidence_count,
            printable_ratio=0.0,
            suspicious_character_ratio=0.0,
            reasons=['no_text'],
        )

    non_space_characters = [character for character in normalized if not character.isspace()]
    printable_characters = [
        character for character in non_space_characters if character.isprintable()
    ]
    suspicious_characters = [
        character
        for character in non_space_characters
        if character == '\ufffd' or unicodedata.category(character).startswith('C')
    ]
    non_space_count = len(non_space_characters)
    printable_ratio = len(printable_characters) / max(1, non_space_count)
    suspicious_character_ratio = len(suspicious_characters) / max(1, non_space_count)

    bounded_confidence = (
        max(0.0, min(1.0, float(average_confidence)))
        if average_confidence is not None
        else None
    )
    # Directly parsed text has no OCR confidence. Treat missing confidence as
    # neutral rather than incorrectly marking valid text as low quality.
    confidence_for_scoring = bounded_confidence if bounded_confidence is not None else 0.85
    confidence_score = confidence_for_scoring * 65.0
    printable_score = printable_ratio * 20.0
    content_score = min(10.0, non_space_count / 4.0)
    character_safety_score = (1.0 - suspicious_character_ratio) * 5.0
    empty_page_penalty = (
        normalized_empty_page_count / normalized_page_count
    ) * 25.0
    low_confidence_penalty = (
        normalized_low_confidence_count / normalized_region_count
        if normalized_region_count
        else 0.0
    ) * 15.0
    score = max(
        0.0,
        min(
            100.0,
            confidence_score
            + printable_score
            + content_score
            + character_safety_score
            - empty_page_penalty
            - low_confidence_penalty,
        ),
    )

    reasons = []
    if bounded_confidence is None:
        reasons.append('confidence_unavailable')
    elif bounded_confidence < OCR_LOW_CONFIDENCE_REGION_THRESHOLD:
        reasons.append('very_low_average_confidence')
    elif bounded_confidence < OCR_ENHANCEMENT_RETRY_CONFIDENCE:
        reasons.append('low_average_confidence')
    if normalized_empty_page_count:
        reasons.append('empty_pages')
    if normalized_low_confidence_count:
        reasons.append('low_confidence_regions')
    if printable_ratio < 0.98:
        reasons.append('non_printable_characters')
    if suspicious_character_ratio > 0:
        reasons.append('suspicious_characters')
    if non_space_count < 4:
        reasons.append('very_short_text')

    if score >= OCR_HIGH_QUALITY_SCORE:
        status = 'high'
    elif score >= OCR_REVIEW_QUALITY_SCORE:
        status = 'review'
    else:
        status = 'low'

    return OCRQualityAssessment(
        score=score,
        status=status,
        average_confidence=bounded_confidence,
        character_count=len(normalized),
        page_count=normalized_page_count,
        region_count=normalized_region_count,
        empty_page_count=normalized_empty_page_count,
        low_confidence_region_count=normalized_low_confidence_count,
        printable_ratio=printable_ratio,
        suspicious_character_ratio=suspicious_character_ratio,
        reasons=reasons,
    )


def score_ocr_text(text: str, average_confidence: float | None = None) -> float:
    """Rank alternate OCR attempts using document-neutral quality signals."""

    assessment = assess_ocr_text(text, average_confidence)
    return assessment.score + min(50.0, assessment.character_count / 10.0)


def _extract_best_ocr_text_from_image(
    reader,
    image: Image.Image,
    rotations: Sequence[int] = OCR_ROTATIONS,
) -> tuple[str, float, int]:
    best_text = ''
    best_score = float('-inf')
    best_rotation = 0

    for rotation in rotations:
        rotated = image if rotation == 0 else image.rotate(rotation, expand=True)
        try:
            page_result = reader.readtext(
                np.array(rotated),
                detail=1,
                paragraph=False,
            )
        finally:
            if rotation != 0:
                rotated.close()

        candidate_text = '\n'.join(
            str(item[1]).strip()
            for item in page_result
            if len(item) >= 2 and str(item[1]).strip()
        ).strip()
        confidence_weights = [
            (float(item[2]), max(1, len(str(item[1]).strip())))
            for item in page_result
            if len(item) >= 3 and str(item[1]).strip()
        ]
        confidence_weight = sum(weight for _, weight in confidence_weights)
        average_confidence = (
            sum(confidence * weight for confidence, weight in confidence_weights)
            / confidence_weight
            if confidence_weight
            else None
        )
        candidate_score = score_ocr_text(candidate_text, average_confidence)

        if candidate_score > best_score:
            best_text = candidate_text
            best_score = candidate_score
            best_rotation = rotation

    return best_text, best_score, best_rotation


def _polygon_bounds(
    polygon: Sequence[Sequence[float]],
) -> tuple[float, float, float, float]:
    xs = [point[0] for point in polygon]
    ys = [point[1] for point in polygon]
    return min(xs), min(ys), max(xs), max(ys)


def _paragraph_confidence(
    paragraph_polygon: Sequence[Sequence[float]],
    word_regions: Sequence[Sequence[object]],
) -> float | None:
    """Average confidence of words whose centers fall inside a paragraph."""

    left, top, right, bottom = _polygon_bounds(paragraph_polygon)
    matching_confidences = []
    for word_polygon, _, confidence in word_regions:
        word_left, word_top, word_right, word_bottom = _polygon_bounds(word_polygon)
        center_x = (word_left + word_right) / 2
        center_y = (word_top + word_bottom) / 2
        if left <= center_x <= right and top <= center_y <= bottom:
            matching_confidences.append(float(confidence))
    if not matching_confidences:
        return None
    return sum(matching_confidences) / len(matching_confidences)


def _upscale_for_ocr(image: Image.Image) -> tuple[Image.Image, float]:
    longest_edge = max(image.width, image.height)
    if longest_edge >= OCR_MIN_LONG_EDGE:
        return image.copy(), 1.0
    scale = OCR_MIN_LONG_EDGE / longest_edge
    return (
        image.resize(
            (
                max(1, round(image.width * scale)),
                max(1, round(image.height * scale)),
            ),
            Image.Resampling.LANCZOS,
        ),
        scale,
    )


def _enhance_for_ocr(image: Image.Image) -> Image.Image:
    image_array = np.array(image.convert('RGB'))
    lab = cv2.cvtColor(image_array, cv2.COLOR_RGB2LAB)
    luminance, channel_a, channel_b = cv2.split(lab)
    enhanced_luminance = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8)).apply(
        luminance
    )
    enhanced = cv2.cvtColor(
        cv2.merge((enhanced_luminance, channel_a, channel_b)),
        cv2.COLOR_LAB2RGB,
    )
    sharpened = cv2.addWeighted(
        enhanced,
        1.35,
        cv2.GaussianBlur(enhanced, (0, 0), 1.0),
        -0.35,
        0,
    )
    return Image.fromarray(sharpened)


def _regions_average_confidence(
    regions: Sequence[DocumentOCRRegion],
) -> float | None:
    weighted_confidences = [
        (region.confidence, max(1, len(region.text)))
        for region in regions
        if region.confidence is not None and region.text
    ]
    total_weight = sum(weight for _, weight in weighted_confidences)
    if not total_weight:
        return None
    return sum(
        float(confidence) * weight
        for confidence, weight in weighted_confidences
    ) / total_weight


def assess_ocr_result(result: DocumentOCRResult) -> OCRQualityAssessment:
    """Assess a complete positioned result using only generic OCR evidence."""

    regions = [
        region
        for page in result.pages
        for region in (page.words or page.regions)
    ]
    low_confidence_region_count = sum(
        region.confidence is not None
        and region.confidence < OCR_LOW_CONFIDENCE_REGION_THRESHOLD
        for region in regions
    )
    assessment = assess_ocr_text(
        result.text,
        _regions_average_confidence(regions),
        page_count=len(result.pages),
        region_count=len(regions),
        empty_page_count=sum(not page.text for page in result.pages),
        low_confidence_region_count=low_confidence_region_count,
    )
    page_quality_scores = [
        page.image_quality_score
        for page in result.pages
        if page.image_quality_score is not None
    ]
    assessment.provider_quality_score = (
        min(page_quality_scores) if page_quality_scores else None
    )
    assessment.quality_defects = [
        {
            **defect,
            'page_number': page.page_number,
        }
        for page in result.pages
        for defect in page.quality_defects
    ]
    if assessment.provider_quality_score is not None:
        if assessment.provider_quality_score < OCR_REVIEW_PROVIDER_QUALITY_SCORE:
            assessment.reasons.append('provider_low_image_quality')
            if assessment.status == 'high':
                assessment.status = 'review'
        if assessment.provider_quality_score < OCR_LOW_PROVIDER_QUALITY_SCORE:
            assessment.status = 'low'

    material_readability_defects = []
    severe_readability_defects = []
    for defect in assessment.quality_defects:
        defect_type = str(defect.get('type') or '')
        confidence = float(defect.get('confidence') or 0.0)
        if (
            defect_type in OCR_SEVERE_READABILITY_DEFECTS
            and confidence >= OCR_REVIEW_DEFECT_CONFIDENCE
        ):
            material_readability_defects.append(defect_type)
            if confidence >= OCR_LOW_DEFECT_CONFIDENCE:
                severe_readability_defects.append(defect_type)
        elif (
            defect_type in OCR_REVIEW_ONLY_DEFECTS
            and confidence >= OCR_REVIEW_DEFECT_CONFIDENCE
        ):
            material_readability_defects.append(defect_type)

    for defect_type in sorted(set(material_readability_defects)):
        reason_name = defect_type.rsplit('/', 1)[-1].removeprefix('defect_')
        assessment.reasons.append(f'provider_quality_defect_{reason_name}')
    if material_readability_defects and assessment.status == 'high':
        assessment.status = 'review'
    if len(set(severe_readability_defects)) >= 2:
        assessment.reasons.append('multiple_severe_readability_defects')
        assessment.status = 'low'

    assessment.reasons = list(dict.fromkeys(assessment.reasons))
    return assessment


def _build_ocr_result(
    pages: list[DocumentOCRPage],
    *,
    file_bytes: bytes,
    languages: Sequence[str],
    device: str,
) -> DocumentOCRResult:
    result = DocumentOCRResult(
        pages=pages,
        device=device,
        languages=list(languages),
        source_hash=hashlib.sha256(file_bytes).hexdigest(),
    )
    result.quality = assess_ocr_result(result)
    return result


def _recognize_positioned_regions(
    reader,
    image: Image.Image,
) -> tuple[list[DocumentOCRRegion], list[DocumentOCRRegion]]:
    """Recognize paragraphs while retaining word-level text and coordinates."""

    image_array = np.array(image)
    image_bgr, image_gray = reformat_input(image_array)
    horizontal_lists, free_lists = reader.detect(
        image_bgr,
        reformat=False,
    )
    horizontal_list = horizontal_lists[0]
    free_list = free_lists[0]
    paragraph_regions = reader.recognize(
        image_gray,
        horizontal_list,
        free_list,
        detail=1,
        paragraph=True,
        reformat=False,
    )
    word_regions = reader.recognize(
        image_gray,
        horizontal_list,
        free_list,
        detail=1,
        paragraph=False,
        reformat=False,
    )

    paragraphs = [
        DocumentOCRRegion(
            polygon=[[float(x), float(y)] for x, y in polygon],
            text=str(text).strip(),
            confidence=_paragraph_confidence(polygon, word_regions),
        )
        for polygon, text, *_ in paragraph_regions
        if str(text).strip()
    ]
    words = [
        DocumentOCRRegion(
            polygon=[[float(x), float(y)] for x, y in polygon],
            text=str(text).strip(),
            confidence=float(confidence),
        )
        for polygon, text, confidence, *_ in word_regions
        if str(text).strip()
    ]
    return paragraphs, words


def _recognize_positioned_page(
    reader,
    image: Image.Image,
    rotations: Sequence[int],
) -> tuple[
    list[DocumentOCRRegion],
    list[DocumentOCRRegion],
    float,
    float,
    int,
]:
    positioned_image = image
    rotation = 0
    try:
        if len(rotations) > 1 or rotations[0] != 0:
            _, _, rotation = _extract_best_ocr_text_from_image(
                reader,
                image,
                rotations,
            )
            if rotation:
                positioned_image = image.rotate(rotation, expand=True)

        regions, words = _recognize_positioned_regions(reader, positioned_image)
        return (
            regions,
            words,
            float(positioned_image.width),
            float(positioned_image.height),
            rotation,
        )
    finally:
        if positioned_image is not image:
            positioned_image.close()


def _recognize_page_with_fallback(
    reader,
    image: Image.Image,
    rotations: Sequence[int],
    languages: Sequence[str],
    configured_device: str,
    page_number: int,
) -> tuple[
    list[DocumentOCRRegion],
    list[DocumentOCRRegion],
    float,
    float,
    int,
    object,
]:
    try:
        regions, words, page_width, page_height, rotation = (
            _recognize_positioned_page(reader, image, rotations)
        )
    except Exception as exc:
        active_device = str(getattr(reader, 'device', configured_device))
        if active_device == 'cpu':
            raise
        logger.warning(
            f'OCR page {page_number} failed on device={active_device}: '
            f'{exc}. Retrying on CPU.'
        )
        reader = _get_easyocr_reader(tuple(languages), 'cpu')
        regions, words, page_width, page_height, rotation = (
            _recognize_positioned_page(reader, image, rotations)
        )

    return regions, words, page_width, page_height, rotation, reader


def _recognize_raster_candidate(
    reader,
    image: Image.Image,
    rotations: Sequence[int],
    languages: Sequence[str],
    device: str,
    page_number: int,
    source_scale: float,
    enhance_low_quality: bool,
) -> tuple[DocumentOCRPage, object, OCRQualityAssessment]:
    prepared_image, upscale = _upscale_for_ocr(image)
    candidate_rotations = tuple(rotations)

    try:
        regions, words, page_width, page_height, rotation, reader = (
            _recognize_page_with_fallback(
                reader,
                prepared_image,
                candidate_rotations,
                languages,
                device,
                page_number,
            )
        )
        average_confidence = _regions_average_confidence(regions)
        text = '\n'.join(region.text for region in regions if region.text).strip()
        assessment = assess_ocr_text(text, average_confidence)

        should_retry_enhanced = (
            enhance_low_quality
            and (
                assessment.status != 'high'
                or average_confidence is None
                or average_confidence < OCR_ENHANCEMENT_RETRY_CONFIDENCE
            )
        )
        if should_retry_enhanced:
            enhanced_image = _enhance_for_ocr(prepared_image)
            try:
                (
                    enhanced_regions,
                    enhanced_words,
                    enhanced_width,
                    enhanced_height,
                    _,
                    reader,
                ) = (
                    _recognize_page_with_fallback(
                        reader,
                        enhanced_image,
                        (rotation,),
                        languages,
                        device,
                        page_number,
                    )
                )
            finally:
                enhanced_image.close()

            enhanced_confidence = _regions_average_confidence(enhanced_regions)
            enhanced_text = '\n'.join(
                region.text for region in enhanced_regions if region.text
            ).strip()
            enhanced_assessment = assess_ocr_text(
                enhanced_text,
                enhanced_confidence,
            )
            if score_ocr_text(
                enhanced_text,
                enhanced_confidence,
            ) > score_ocr_text(text, average_confidence):
                regions = enhanced_regions
                words = enhanced_words
                page_width = enhanced_width
                page_height = enhanced_height
                assessment = enhanced_assessment

        return (
            DocumentOCRPage(
                page_number=page_number,
                regions=regions,
                width=page_width,
                height=page_height,
                render_scale=source_scale * upscale,
                rotation=rotation,
                words=words,
            ),
            reader,
            assessment,
        )
    finally:
        prepared_image.close()


def _ocr_cache_path(
    file_bytes: bytes,
    languages: Sequence[str],
    render_dpi: int,
    rotations: Sequence[int],
    mime_type: str,
    enhance_low_quality: bool,
    max_render_dimension: Optional[int] = None,
) -> Path:
    document_hash = hashlib.sha256(file_bytes).hexdigest()[:20]
    normalized_rotations = tuple(int(rotation) for rotation in rotations)

    settings = json.dumps(
        {
            'languages': list(languages),
            'mime_type': mime_type,
            'enhance_low_quality': enhance_low_quality,
            'dpi': render_dpi,
            'rotations': normalized_rotations,
            'max_render_dimension': max_render_dimension,
            'version': POSITIONED_OCR_CACHE_VERSION,
        },
        sort_keys=True,
    ).encode('utf-8')
    settings_hash = hashlib.sha256(settings).hexdigest()[:12]
    return (
        POSITIONED_OCR_CACHE_DIRECTORY
        / f'{document_hash}_{settings_hash}_regions_{POSITIONED_OCR_CACHE_VERSION}.json'
    )


def _serialize_ocr_page(page: DocumentOCRPage) -> dict:
    return {
        'page_number': page.page_number,
        'width': page.width,
        'height': page.height,
        'render_scale': page.render_scale,
        'rotation': page.rotation,
        'source_text': page.source_text,
        'image_quality_score': page.image_quality_score,
        'quality_defects': page.quality_defects,
        'regions': [
            {
                'polygon': region.polygon,
                'text': region.text,
                'confidence': region.confidence,
            }
            for region in page.regions
        ],
        'lines': [
            {
                'polygon': line.polygon,
                'text': line.text,
                'confidence': line.confidence,
            }
            for line in page.lines
        ],
        'words': [
            {
                'polygon': word.polygon,
                'text': word.text,
                'confidence': word.confidence,
            }
            for word in page.words
        ],
        'symbols': [
            {
                'polygon': symbol.polygon,
                'text': symbol.text,
                'confidence': symbol.confidence,
            }
            for symbol in page.symbols
        ],
    }


def _deserialize_ocr_page(payload: dict, page_number: int) -> DocumentOCRPage:
    if not isinstance(payload, dict):
        raise ValueError(f'Invalid OCR cache payload for page {page_number}.')
    regions = payload.get('regions')
    if not isinstance(regions, list):
        raise ValueError(f'Invalid OCR cache regions for page {page_number}.')
    def _deserialize_regions(key: str) -> list[DocumentOCRRegion]:
        raw_regions = payload.get(key) or []
        if not isinstance(raw_regions, list):
            raise ValueError(
                f'Invalid OCR cache {key} for page {page_number}.'
            )
        return [
            DocumentOCRRegion(
                polygon=[
                    [float(point[0]), float(point[1])]
                    for point in region['polygon']
                ],
                text=str(region.get('text') or '').strip(),
                confidence=(
                    float(region['confidence'])
                    if region.get('confidence') is not None
                    else None
                ),
            )
            for region in raw_regions
            if str(region.get('text') or '').strip()
        ]

    return DocumentOCRPage(
        page_number=page_number,
        regions=_deserialize_regions('regions'),
        width=float(payload['width']),
        height=float(payload['height']),
        render_scale=float(payload['render_scale']),
        rotation=int(payload.get('rotation', 0)),
        source_text=str(payload.get('source_text') or '').strip() or None,
        lines=_deserialize_regions('lines'),
        words=_deserialize_regions('words'),
        symbols=_deserialize_regions('symbols'),
        image_quality_score=(
            float(payload['image_quality_score'])
            if payload.get('image_quality_score') is not None
            else None
        ),
        quality_defects=list(payload.get('quality_defects') or []),
    )


def _read_ocr_cache(cache_path: Path) -> tuple[dict[str, dict], str | None]:
    try:
        payload = json.loads(cache_path.read_text(encoding='utf-8'))
    except (OSError, ValueError, TypeError) as exc:
        logger.warning(f'Ignoring unreadable OCR cache {cache_path}: {exc}')
        return {}, None

    if not isinstance(payload, dict):
        logger.warning(f'Ignoring invalid OCR cache payload: {cache_path}')
        return {}, None
    if payload.get('pipeline_version') != OCR_PIPELINE_VERSION:
        logger.info(f'Ignoring stale OCR cache version: {cache_path}')
        return {}, None
    pages = payload.get('pages')
    if not isinstance(pages, dict):
        logger.warning(f'Ignoring OCR cache without a pages mapping: {cache_path}')
        return {}, None
    cached_device = str(payload.get('device') or '').strip() or None
    return pages, cached_device


def _write_ocr_cache(
    cache_path: Path,
    pages: dict[str, dict],
    *,
    device: str,
    languages: Sequence[str],
) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = cache_path.with_suffix(
        f'.{os.getpid()}.{threading.get_ident()}.tmp'
    )
    payload = {
        'pipeline_version': OCR_PIPELINE_VERSION,
        'provider': POSITIONED_OCR_PROVIDER,
        'model_version': EASYOCR_VERSION,
        'device': device,
        'languages': list(languages),
        'pages': pages,
    }
    try:
        temporary_path.write_text(
            json.dumps(payload, ensure_ascii=False),
            encoding='utf-8',
        )
        temporary_path.replace(cache_path)
    finally:
        if temporary_path.exists():
            temporary_path.unlink()


def _normalize_raster_frame(frame: Image.Image) -> Image.Image:
    oriented_image = ImageOps.exif_transpose(frame)
    try:
        has_transparency = (
            'A' in oriented_image.getbands()
            or 'transparency' in oriented_image.info
        )
        if not has_transparency:
            return oriented_image.convert('RGB')

        rgba_image = oriented_image.convert('RGBA')
        try:
            normalized_image = Image.new('RGB', rgba_image.size, 'white')
            normalized_image.paste(rgba_image, mask=rgba_image.getchannel('A'))
            return normalized_image
        finally:
            rgba_image.close()
    finally:
        if oriented_image is not frame:
            oriented_image.close()


def _extract_raster_image_ocr(
    file_bytes: bytes,
    languages: Sequence[str],
    device: str,
    rotations: Sequence[int],
    max_render_dimension: Optional[int],
    cached_pages: dict[str, dict],
    cache_path: Path,
    use_cache: bool,
    enhance_low_quality: bool,
    max_pages: int,
    max_image_pixels: int,
    page_numbers: Sequence[int] | None,
    cached_device: str | None,
) -> DocumentOCRResult:
    try:
        source_image = Image.open(BytesIO(file_bytes))
    except Exception as exc:
        raise ValueError(f'Failed to open image for OCR: {exc}') from exc

    try:
        frame_count = int(getattr(source_image, 'n_frames', 1))
    except Exception as exc:
        source_image.close()
        raise ValueError(f'Failed to inspect image pages: {exc}') from exc
    if frame_count > max_pages:
        source_image.close()
        raise ValueError(
            f'Document exceeds OCR page limit: {frame_count} pages '
            f'(maximum {max_pages}).'
        )

    available_page_numbers = list(range(1, frame_count + 1))
    selected_page_numbers = (
        list(page_numbers) if page_numbers is not None else available_page_numbers
    )
    invalid_page_numbers = [
        page_number
        for page_number in selected_page_numbers
        if page_number not in available_page_numbers
    ]
    if invalid_page_numbers:
        source_image.close()
        raise ValueError(
            f'OCR page selection is outside the raster document: '
            f'{invalid_page_numbers}.'
        )
    missing_page_numbers = [
        page_number
        for page_number in selected_page_numbers
        if str(page_number) not in cached_pages
    ]
    reader = (
        _get_easyocr_reader(tuple(languages), device)
        if missing_page_numbers
        else None
    )
    pages: list[DocumentOCRPage] = []
    try:
        for page_number in selected_page_numbers:
            cached_page = cached_pages.get(str(page_number))
            if cached_page is not None:
                try:
                    pages.append(_deserialize_ocr_page(cached_page, page_number))
                except (KeyError, TypeError, ValueError) as exc:
                    logger.warning(
                        f'Ignoring invalid OCR cache page {page_number}: {exc}'
                    )
                else:
                    continue

            try:
                source_image.seek(page_number - 1)
                frame = source_image.copy()
                try:
                    source_page = _normalize_raster_frame(frame)
                finally:
                    frame.close()
            except Exception as exc:
                raise ValueError(
                    f'Failed to decode image page {page_number}: {exc}'
                ) from exc

            pixel_count = source_page.width * source_page.height
            if pixel_count > max_image_pixels:
                source_page.close()
                raise ValueError(
                    f'Image page {page_number} exceeds OCR pixel limit: '
                    f'{pixel_count} pixels (maximum {max_image_pixels}).'
                )

            render_scale = 1.0
            processing_page = source_page
            largest_dimension = max(source_page.width, source_page.height)
            if max_render_dimension and largest_dimension > max_render_dimension:
                render_scale = max_render_dimension / largest_dimension
                processing_page = source_page.resize(
                    (
                        max(1, round(source_page.width * render_scale)),
                        max(1, round(source_page.height * render_scale)),
                    ),
                    Image.Resampling.LANCZOS,
                )

            logger.info(
                f'Raster OCR page {page_number}: '
                f'image={processing_page.width}x{processing_page.height} '
                f'enhance_low_quality={enhance_low_quality}'
            )
            try:
                page, reader, assessment = _recognize_raster_candidate(
                    reader,
                    processing_page,
                    rotations,
                    languages,
                    device,
                    page_number,
                    render_scale,
                    enhance_low_quality,
                )
            finally:
                if processing_page is not source_page:
                    processing_page.close()
                source_page.close()

            logger.info(
                f'Raster OCR page {page_number}: chars={len(page.text)} '
                f'quality={assessment.status} score={assessment.score:.1f} '
                f'confidence={assessment.average_confidence}'
            )
            pages.append(page)
            cached_pages[str(page_number)] = _serialize_ocr_page(page)
            if use_cache:
                _write_ocr_cache(
                    cache_path,
                    cached_pages,
                    device=_active_reader_device(reader, device),
                    languages=languages,
                )
    finally:
        source_image.close()

    result = _build_ocr_result(
        pages,
        file_bytes=file_bytes,
        languages=languages,
        device=_active_reader_device(reader, cached_device or device),
    )
    if not result.text:
        raise ValueError('Image OCR completed but produced no positioned text.')
    return result


def _extract_document_ocr_easyocr(
    file_bytes: bytes,
    source_language: Optional[str] = None,
    *,
    mime_type: str = 'application/pdf',
    languages: Optional[Sequence[str]] = None,
    ocr_device: Optional[str] = None,
    render_dpi: int = POSITIONED_OCR_DPI,
    rotations: Sequence[int] = OCR_ROTATIONS,
    max_render_dimension: Optional[int] = None,
    enhance_low_quality: bool = True,
    use_cache: bool = True,
    max_file_bytes: int = OCR_MAX_FILE_BYTES,
    max_pages: int = OCR_MAX_PAGES,
    max_image_pixels: int = OCR_MAX_IMAGE_PIXELS,
    page_numbers: Optional[Sequence[int]] = None,
) -> DocumentOCRResult:
    """Extract positioned text from a PDF or image without business analysis.

    This is the shared OCR boundary for every consumer. It returns text,
    positions, confidence, generic quality signals, and reproducibility
    metadata. Translation and document-specific field extraction belong
    downstream and must not be implemented here.
    """

    if not file_bytes:
        raise ValueError('Document data is empty.')
    if max_file_bytes <= 0:
        raise ValueError('max_file_bytes must be greater than zero.')
    if len(file_bytes) > max_file_bytes:
        raise ValueError(
            f'Document exceeds OCR file limit: {len(file_bytes)} bytes '
            f'(maximum {max_file_bytes}).'
        )
    if max_pages <= 0:
        raise ValueError('max_pages must be greater than zero.')
    if max_image_pixels <= 0:
        raise ValueError('max_image_pixels must be greater than zero.')
    if render_dpi <= 0:
        raise ValueError('render_dpi must be greater than zero.')
    if max_render_dimension is not None and max_render_dimension <= 0:
        raise ValueError('max_render_dimension must be greater than zero.')

    normalized_mime_type = str(mime_type or '').split(';', 1)[0].strip().lower()
    if normalized_mime_type not in {
        'application/pdf',
        *SUPPORTED_RASTER_MIME_TYPES,
    }:
        raise ValueError(
            f'Unsupported OCR mime type: {normalized_mime_type or "unknown"}'
        )

    selected_languages = list(languages or _get_easyocr_languages(source_language))
    selected_languages = list(dict.fromkeys(selected_languages))
    if not selected_languages:
        raise ValueError('At least one OCR language is required.')

    selected_rotations = tuple(int(rotation) for rotation in rotations)
    if not selected_rotations or any(
        rotation not in OCR_ROTATIONS for rotation in selected_rotations
    ):
        raise ValueError('rotations must contain one or more of 0, 90, 180, or 270.')

    selected_page_numbers = None
    if page_numbers is not None:
        selected_page_numbers = sorted({int(page_number) for page_number in page_numbers})
        if not selected_page_numbers or any(
            page_number <= 0 for page_number in selected_page_numbers
        ):
            raise ValueError('page_numbers must contain positive page numbers.')

    device = _resolve_easyocr_device(ocr_device)
    cache_path = _ocr_cache_path(
        file_bytes,
        selected_languages,
        render_dpi,
        selected_rotations,
        normalized_mime_type,
        enhance_low_quality,
        max_render_dimension=max_render_dimension,
    )
    cached_pages: dict[str, dict] = {}
    cached_device = None
    if use_cache and cache_path.exists():
        cached_pages, cached_device = _read_ocr_cache(cache_path)

    if normalized_mime_type in SUPPORTED_RASTER_MIME_TYPES:
        return _extract_raster_image_ocr(
            file_bytes=file_bytes,
            languages=selected_languages,
            device=device,
            rotations=selected_rotations,
            max_render_dimension=max_render_dimension,
            cached_pages=cached_pages,
            cache_path=cache_path,
            use_cache=use_cache,
            enhance_low_quality=enhance_low_quality,
            max_pages=max_pages,
            max_image_pixels=max_image_pixels,
            page_numbers=selected_page_numbers,
            cached_device=cached_device,
        )

    try:
        pdf = pypdfium2.PdfDocument(BytesIO(file_bytes))
    except Exception as exc:
        raise ValueError(f'Failed to open PDF for OCR rendering: {exc}') from exc

    pdf_page_count = len(pdf)
    if pdf_page_count > max_pages:
        pdf.close()
        raise ValueError(
            f'Document exceeds OCR page limit: {pdf_page_count} pages '
            f'(maximum {max_pages}).'
        )

    pdf_page_numbers = (
        selected_page_numbers
        if selected_page_numbers is not None
        else list(range(1, pdf_page_count + 1))
    )
    invalid_page_numbers = [
        page_number
        for page_number in pdf_page_numbers
        if page_number > pdf_page_count
    ]
    if invalid_page_numbers:
        pdf.close()
        raise ValueError(
            f'OCR page selection is outside the PDF: {invalid_page_numbers}.'
        )

    missing_pages = [
        page_number
        for page_number in pdf_page_numbers
        if str(page_number) not in cached_pages
    ]
    reader = None
    if missing_pages:
        reader = _get_easyocr_reader(tuple(selected_languages), device)

    default_scale = render_dpi / 72
    pages: list[DocumentOCRPage] = []
    try:
        for page_number in pdf_page_numbers:
            page = pdf[page_number - 1]
            largest_page_dimension = max(float(page.get_width()), float(page.get_height()))
            scale = default_scale
            if max_render_dimension and largest_page_dimension > 0:
                scale = min(scale, max_render_dimension / largest_page_dimension)

            rendered_width = max(1, round(float(page.get_width()) * scale))
            rendered_height = max(1, round(float(page.get_height()) * scale))
            rendered_pixels = rendered_width * rendered_height
            if rendered_pixels > max_image_pixels:
                page.close()
                raise ValueError(
                    f'PDF page {page_number} exceeds OCR pixel limit after '
                    f'rendering: {rendered_pixels} pixels '
                    f'(maximum {max_image_pixels}).'
                )

            cached_page = cached_pages.get(str(page_number))
            if cached_page is not None:
                try:
                    pages.append(_deserialize_ocr_page(cached_page, page_number))
                except (KeyError, TypeError, ValueError) as exc:
                    logger.warning(
                        f'Ignoring invalid OCR cache page {page_number}: {exc}'
                    )
                else:
                    page.close()
                    continue

            bitmap = page.render(scale=scale)
            image: Image.Image = bitmap.to_pil()
            try:
                regions, words, page_width, page_height, rotation, reader = (
                    _recognize_page_with_fallback(
                        reader,
                        image,
                        selected_rotations,
                        selected_languages,
                        device,
                        page_number,
                    )
                )
            finally:
                image.close()
                page.close()

            ocr_page = DocumentOCRPage(
                page_number=page_number,
                regions=regions,
                width=page_width,
                height=page_height,
                render_scale=scale,
                rotation=rotation,
                words=words,
            )
            pages.append(ocr_page)
            cached_pages[str(page_number)] = _serialize_ocr_page(ocr_page)
            if use_cache:
                _write_ocr_cache(
                    cache_path,
                    cached_pages,
                    device=_active_reader_device(reader, device),
                    languages=selected_languages,
                )
    finally:
        pdf.close()

    result = _build_ocr_result(
        pages,
        file_bytes=file_bytes,
        languages=selected_languages,
        device=_active_reader_device(reader, cached_device or device),
    )
    if not result.text:
        raise ValueError('PDF OCR completed but produced no positioned text.')
    return result


def _document_ai_layout_text(document_text: str, layout) -> str:
    text_anchor = getattr(layout, 'text_anchor', None)
    segments = list(getattr(text_anchor, 'text_segments', []) or [])
    pieces = []
    for segment in segments:
        start_index = int(getattr(segment, 'start_index', 0) or 0)
        end_index = int(getattr(segment, 'end_index', 0) or 0)
        if end_index > start_index:
            pieces.append(document_text[start_index:end_index])
    return ''.join(pieces).strip()


def _document_ai_polygon(layout, width: float, height: float) -> list[list[float]]:
    bounding_poly = getattr(layout, 'bounding_poly', None)
    normalized_vertices = list(
        getattr(bounding_poly, 'normalized_vertices', []) or []
    )
    if normalized_vertices:
        return [
            [
                float(getattr(vertex, 'x', 0.0) or 0.0) * width,
                float(getattr(vertex, 'y', 0.0) or 0.0) * height,
            ]
            for vertex in normalized_vertices
        ]
    return [
        [
            float(getattr(vertex, 'x', 0.0) or 0.0),
            float(getattr(vertex, 'y', 0.0) or 0.0),
        ]
        for vertex in list(getattr(bounding_poly, 'vertices', []) or [])
    ]


def _document_ai_region(
    item,
    document_text: str,
    width: float,
    height: float,
) -> DocumentOCRRegion | None:
    layout = getattr(item, 'layout', None)
    if layout is None:
        return None
    text = _document_ai_layout_text(document_text, layout)
    if not text:
        return None
    return DocumentOCRRegion(
        polygon=_document_ai_polygon(layout, width, height),
        text=text,
        confidence=float(getattr(layout, 'confidence', 0.0) or 0.0),
    )


def _document_ai_regions(
    items,
    document_text: str,
    width: float,
    height: float,
) -> list[DocumentOCRRegion]:
    regions = []
    for item in list(items or []):
        region = _document_ai_region(item, document_text, width, height)
        if region is not None:
            regions.append(region)
    return regions


def _document_ai_quality(page) -> tuple[float | None, list[dict]]:
    quality = getattr(page, 'image_quality_scores', None)
    if quality is None:
        return None, []
    score = getattr(quality, 'quality_score', None)
    defects = [
        {
            'type': str(getattr(defect, 'type_', None) or getattr(defect, 'type', '')),
            'confidence': float(getattr(defect, 'confidence', 0.0) or 0.0),
        }
        for defect in list(getattr(quality, 'detected_defects', []) or [])
    ]
    return (float(score) if score is not None else None), defects


def _google_document_ai_cache_path(
    request: DocumentOCRRequest,
    processor_name: str,
) -> Path:
    document_hash = hashlib.sha256(request.file_bytes).hexdigest()[:20]
    settings = json.dumps(
        {
            'provider': GOOGLE_DOCUMENT_AI_PROVIDER,
            'processor': processor_name,
            'languages': request.languages,
            'mime_type': request.mime_type,
            'page_numbers': request.page_numbers,
            'version': OCR_PIPELINE_VERSION,
        },
        sort_keys=True,
    ).encode('utf-8')
    settings_hash = hashlib.sha256(settings).hexdigest()[:12]
    return (
        POSITIONED_OCR_CACHE_DIRECTORY
        / f'{document_hash}_{settings_hash}_google_{OCR_PIPELINE_VERSION}.json'
    )


def _read_google_document_ai_cache(
    cache_path: Path,
    file_bytes: bytes,
) -> DocumentOCRResult | None:
    try:
        payload = json.loads(cache_path.read_text(encoding='utf-8'))
        if payload.get('pipeline_version') != OCR_PIPELINE_VERSION:
            return None
        pages = [
            _deserialize_ocr_page(page, index)
            for index, page in enumerate(payload.get('pages') or [], start=1)
        ]
        if not pages:
            return None
        result = DocumentOCRResult(
            pages=pages,
            provider=GOOGLE_DOCUMENT_AI_PROVIDER,
            model_version=str(payload.get('model_version') or 'default'),
            device='managed',
            languages=list(payload.get('languages') or []),
            source_hash=hashlib.sha256(file_bytes).hexdigest(),
        )
        result.quality = assess_ocr_result(result)
        return result
    except (KeyError, OSError, TypeError, ValueError) as exc:
        logger.warning(f'Ignoring unreadable Google OCR cache {cache_path}: {exc}')
        return None


def _write_google_document_ai_cache(
    cache_path: Path,
    result: DocumentOCRResult,
) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = cache_path.with_suffix(
        f'.{os.getpid()}.{threading.get_ident()}.tmp'
    )
    payload = {
        'pipeline_version': OCR_PIPELINE_VERSION,
        'provider': result.provider,
        'model_version': result.model_version,
        'languages': result.languages,
        'pages': [_serialize_ocr_page(page) for page in result.pages],
    }
    try:
        temporary_path.write_text(
            json.dumps(payload, ensure_ascii=False),
            encoding='utf-8',
        )
        temporary_path.replace(cache_path)
    finally:
        if temporary_path.exists():
            temporary_path.unlink()


class EasyOCRProvider:
    name = EASYOCR_PROVIDER

    def validate_configuration(self) -> None:
        return None

    def extract(self, request: DocumentOCRRequest) -> DocumentOCRResult:
        result = _extract_document_ocr_easyocr(
            request.file_bytes,
            source_language=request.source_language,
            mime_type=request.mime_type,
            languages=request.languages,
            ocr_device=request.ocr_device,
            render_dpi=request.render_dpi,
            rotations=request.rotations,
            max_render_dimension=request.max_render_dimension,
            enhance_low_quality=request.enhance_low_quality,
            use_cache=request.use_cache,
            max_file_bytes=request.max_file_bytes,
            max_pages=request.max_pages,
            max_image_pixels=request.max_image_pixels,
            page_numbers=request.page_numbers,
        )
        result.provider = self.name
        result.model_version = EASYOCR_VERSION
        return result


class GoogleDocumentAIProvider:
    name = GOOGLE_DOCUMENT_AI_PROVIDER

    def validate_configuration(self) -> None:
        project_id = str(
            os.getenv('GOOGLE_DOCUMENT_AI_PROJECT_ID')
            or os.getenv('GOOGLE_CLOUD_PROJECT')
            or ''
        ).strip()
        missing_settings = [
            name
            for name, value in {
                'GOOGLE_DOCUMENT_AI_PROJECT_ID': project_id,
                'GOOGLE_DOCUMENT_AI_LOCATION': os.getenv(
                    'GOOGLE_DOCUMENT_AI_LOCATION'
                ),
                'GOOGLE_DOCUMENT_AI_PROCESSOR_ID': os.getenv(
                    'GOOGLE_DOCUMENT_AI_PROCESSOR_ID'
                ),
                'GOOGLE_DOCUMENT_AI_PROCESSOR_VERSION': os.getenv(
                    'GOOGLE_DOCUMENT_AI_PROCESSOR_VERSION'
                ),
            }.items()
            if not str(value or '').strip()
        ]
        if missing_settings:
            raise ValueError(
                'Missing Google Document AI configuration: '
                + ', '.join(missing_settings)
            )
        try:
            from google.cloud import documentai  # noqa: F401
        except ImportError as exc:
            raise RuntimeError(
                'Google Document AI OCR requires google-cloud-documentai. '
                'Install the project requirements before using this provider.'
            ) from exc

    def extract(self, request: DocumentOCRRequest) -> DocumentOCRResult:
        project_id = str(
            os.getenv('GOOGLE_DOCUMENT_AI_PROJECT_ID')
            or os.getenv('GOOGLE_CLOUD_PROJECT')
            or ''
        ).strip()
        location = str(os.getenv('GOOGLE_DOCUMENT_AI_LOCATION') or '').strip()
        processor_id = str(
            os.getenv('GOOGLE_DOCUMENT_AI_PROCESSOR_ID') or ''
        ).strip()
        processor_version = str(
            os.getenv('GOOGLE_DOCUMENT_AI_PROCESSOR_VERSION') or ''
        ).strip()
        self.validate_configuration()

        try:
            from google.api_core.client_options import ClientOptions
            from google.cloud import documentai
        except ImportError as exc:
            raise RuntimeError(
                'Google Document AI OCR requires google-cloud-documentai. '
                'Install the project requirements before using this provider.'
            ) from exc

        client = documentai.DocumentProcessorServiceClient(
            client_options=ClientOptions(
                api_endpoint=f'{location}-documentai.googleapis.com'
            )
        )
        processor_name = client.processor_version_path(
            project_id,
            location,
            processor_id,
            processor_version,
        )
        cache_path = _google_document_ai_cache_path(request, processor_name)
        if request.use_cache and cache_path.exists():
            cached_result = _read_google_document_ai_cache(
                cache_path,
                request.file_bytes,
            )
            if cached_result is not None:
                return cached_result

        ocr_config = documentai.OcrConfig(
            enable_native_pdf_parsing=request.mime_type == 'application/pdf',
            enable_image_quality_scores=True,
            enable_symbol=True,
            hints=documentai.OcrConfig.Hints(
                language_hints=request.languages,
            ),
        )
        process_options = documentai.ProcessOptions(ocr_config=ocr_config)
        if request.page_numbers is not None:
            process_options.individual_page_selector = (
                documentai.ProcessOptions.IndividualPageSelector(
                    pages=request.page_numbers
                )
            )
        raw_document = documentai.RawDocument(
            content=request.file_bytes,
            mime_type=request.mime_type,
        )
        timeout_seconds = float(
            os.getenv('GOOGLE_DOCUMENT_AI_TIMEOUT_SECONDS') or 120
        )
        response = client.process_document(
            request=documentai.ProcessRequest(
                name=processor_name,
                raw_document=raw_document,
                process_options=process_options,
            ),
            timeout=timeout_seconds,
        )
        document = response.document
        document_text = str(getattr(document, 'text', '') or '')
        pages = []
        for index, page in enumerate(list(document.pages or []), start=1):
            dimension = getattr(page, 'dimension', None)
            width = float(getattr(dimension, 'width', 0.0) or 0.0)
            height = float(getattr(dimension, 'height', 0.0) or 0.0)
            paragraphs = _document_ai_regions(
                getattr(page, 'paragraphs', []),
                document_text,
                width,
                height,
            )
            lines = _document_ai_regions(
                getattr(page, 'lines', []),
                document_text,
                width,
                height,
            )
            words = _document_ai_regions(
                getattr(page, 'tokens', []),
                document_text,
                width,
                height,
            )
            symbols = _document_ai_regions(
                getattr(page, 'symbols', []),
                document_text,
                width,
                height,
            )
            page_layout = getattr(page, 'layout', None)
            page_text = (
                _document_ai_layout_text(document_text, page_layout)
                if page_layout is not None
                else ''
            )
            image_quality_score, quality_defects = _document_ai_quality(page)
            pages.append(
                DocumentOCRPage(
                    page_number=int(getattr(page, 'page_number', index) or index),
                    regions=paragraphs or lines or words,
                    width=width,
                    height=height,
                    render_scale=1.0,
                    source_text=page_text or None,
                    lines=lines,
                    words=words,
                    symbols=symbols,
                    image_quality_score=image_quality_score,
                    quality_defects=quality_defects,
                )
            )
        if len(pages) > request.max_pages:
            raise ValueError(
                f'Document exceeds OCR page limit: {len(pages)} pages '
                f'(maximum {request.max_pages}).'
            )
        result = _build_ocr_result(
            pages,
            file_bytes=request.file_bytes,
            languages=request.languages,
            device='managed',
        )
        result.provider = self.name
        result.model_version = processor_version
        if not result.text:
            raise ValueError('Google Document AI completed but produced no text.')
        if request.use_cache:
            _write_google_document_ai_cache(cache_path, result)
        return result


_OCR_PROVIDERS: dict[str, DocumentOCRProvider] = {
    EASYOCR_PROVIDER: EasyOCRProvider(),
    GOOGLE_DOCUMENT_AI_PROVIDER: GoogleDocumentAIProvider(),
}
_OCR_PROVIDER_ALIASES = {
    'easyocr': EASYOCR_PROVIDER,
    EASYOCR_PROVIDER: EASYOCR_PROVIDER,
    'google': GOOGLE_DOCUMENT_AI_PROVIDER,
    'google_document_ai': GOOGLE_DOCUMENT_AI_PROVIDER,
    GOOGLE_DOCUMENT_AI_PROVIDER: GOOGLE_DOCUMENT_AI_PROVIDER,
}


def get_ocr_provider(provider: str | None = None) -> DocumentOCRProvider:
    configured_provider = str(
        provider or os.getenv('OCR_PROVIDER') or DEFAULT_OCR_PROVIDER
    ).strip().lower()
    provider_name = _OCR_PROVIDER_ALIASES.get(configured_provider)
    if not provider_name:
        raise ValueError(
            f'Unsupported OCR provider: {configured_provider or "unknown"}. '
            f'Available providers: {sorted(_OCR_PROVIDERS)}'
        )
    return _OCR_PROVIDERS[provider_name]


def validate_ocr_provider_configuration(provider: str | None = None) -> str:
    selected_provider = get_ocr_provider(provider)
    selected_provider.validate_configuration()
    return selected_provider.name


def extract_document_ocr(
    file_bytes: bytes,
    source_language: Optional[str] = None,
    *,
    mime_type: str = 'application/pdf',
    languages: Optional[Sequence[str]] = None,
    provider: str | None = None,
    ocr_device: Optional[str] = None,
    render_dpi: int = POSITIONED_OCR_DPI,
    rotations: Sequence[int] = OCR_ROTATIONS,
    max_render_dimension: Optional[int] = None,
    enhance_low_quality: bool = True,
    use_cache: bool = True,
    max_file_bytes: int = OCR_MAX_FILE_BYTES,
    max_pages: int = OCR_MAX_PAGES,
    max_image_pixels: int = OCR_MAX_IMAGE_PIXELS,
    page_numbers: Optional[Sequence[int]] = None,
) -> DocumentOCRResult:
    """Extract canonical OCR output through the configured provider."""

    if not file_bytes:
        raise ValueError('Document data is empty.')
    if max_file_bytes <= 0:
        raise ValueError('max_file_bytes must be greater than zero.')
    if len(file_bytes) > max_file_bytes:
        raise ValueError(
            f'Document exceeds OCR file limit: {len(file_bytes)} bytes '
            f'(maximum {max_file_bytes}).'
        )
    if max_pages <= 0:
        raise ValueError('max_pages must be greater than zero.')
    if max_image_pixels <= 0:
        raise ValueError('max_image_pixels must be greater than zero.')
    if render_dpi <= 0:
        raise ValueError('render_dpi must be greater than zero.')
    if max_render_dimension is not None and max_render_dimension <= 0:
        raise ValueError('max_render_dimension must be greater than zero.')
    normalized_mime_type = str(mime_type or '').split(';', 1)[0].strip().lower()
    if normalized_mime_type not in {
        'application/pdf',
        *SUPPORTED_RASTER_MIME_TYPES,
    }:
        raise ValueError(
            f'Unsupported OCR mime type: {normalized_mime_type or "unknown"}'
        )
    selected_languages = list(languages or _get_easyocr_languages(source_language))
    selected_languages = list(dict.fromkeys(selected_languages))
    if not selected_languages:
        raise ValueError('At least one OCR language is required.')
    selected_rotations = tuple(int(rotation) for rotation in rotations)
    if not selected_rotations or any(
        rotation not in OCR_ROTATIONS for rotation in selected_rotations
    ):
        raise ValueError('rotations must contain one or more of 0, 90, 180, or 270.')
    selected_page_numbers = (
        sorted({int(page_number) for page_number in page_numbers})
        if page_numbers is not None
        else None
    )
    if selected_page_numbers is not None and (
        not selected_page_numbers
        or any(page_number <= 0 for page_number in selected_page_numbers)
    ):
        raise ValueError('page_numbers must contain positive page numbers.')
    request = DocumentOCRRequest(
        file_bytes=file_bytes,
        source_language=_normalize_language(source_language),
        mime_type=normalized_mime_type,
        languages=selected_languages,
        ocr_device=ocr_device,
        render_dpi=render_dpi,
        rotations=selected_rotations,
        max_render_dimension=max_render_dimension,
        enhance_low_quality=enhance_low_quality,
        use_cache=use_cache,
        max_file_bytes=max_file_bytes,
        max_pages=max_pages,
        max_image_pixels=max_image_pixels,
        page_numbers=selected_page_numbers,
    )
    selected_provider = get_ocr_provider(provider)
    logger.info(f'Running OCR provider={selected_provider.name}')
    return selected_provider.extract(request)


def _extract_text_with_ocr(
    file_bytes: bytes,
    source_language: Optional[str],
    mime_type: str,
) -> tuple[str, str, OCRQualityAssessment]:
    result = extract_document_ocr(
        file_bytes,
        source_language=source_language,
        mime_type=mime_type,
        render_dpi=OCR_RENDER_DPI,
        rotations=OCR_ROTATIONS,
        max_render_dimension=OCR_MAX_RENDER_DIMENSION,
        use_cache=False,
    )
    extracted_text = result.text
    for page in result.pages:
        logger.info(
            f'OCR page {page.page_number}: selected rotation={page.rotation} '
            f'regions={len(page.regions)}'
        )
    assessment = result.quality or assess_ocr_result(result)
    logger.info(
        f'OCR quality: status={assessment.status} score={assessment.score:.1f} '
        f'confidence={assessment.average_confidence} '
        f'provider={result.provider} model={result.model_version} '
        f'pipeline={result.pipeline_version} device={result.device}'
    )
    return extracted_text, result.provider, assessment


def _extract_document_text(
    document_row: dict,
    source_language: Optional[str],
) -> tuple[str, str, OCRQualityAssessment]:
    mime_type = (
        str((document_row or {}).get('mime_type') or '')
        .split(';', 1)[0]
        .strip()
        .lower()
    )
    file_bytes = _decode_document_bytes(document_row)

    if mime_type == 'application/pdf' or mime_type.startswith('image/'):
        ocr_text, provider, assessment = _extract_text_with_ocr(
            file_bytes,
            source_language,
            mime_type,
        )
        if not ocr_text:
            raise ValueError('Document OCR completed but produced no text.')
        return ocr_text, provider, assessment

    if mime_type.startswith('text/'):
        text = _extract_plain_text(file_bytes)
        if not text:
            raise ValueError('Text document is empty after decoding.')
        return text, DIRECT_TEXT_EXTRACTION_PROVIDER, assess_ocr_text(text)

    raise ValueError(f'Unsupported document mime type: {mime_type or "unknown"}')


def _upsert_document_processing_record(
    document_id: str,
    source_language: Optional[str],
    status: str,
    output_text: Optional[str],
    provider: Optional[str],
    error: Optional[str],
):
    db = _get_db()
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


def upsert_document_text_extraction_record(
    document_id: str,
    source_language: Optional[str],
    status: str,
    output_text: Optional[str],
    provider: Optional[str],
    error: Optional[str],
):
    return _upsert_document_processing_record(
        document_id=document_id,
        source_language=source_language,
        status=status,
        output_text=output_text,
        provider=provider,
        error=error,
    )


@handle_exception
def process_document_text_extraction(
    document_id: str,
    source_language: Optional[str] = None,
):
    if not document_id:
        raise ValueError('document_id is required')

    normalized_language = _normalize_language(source_language)
    db = _get_db()
    document_rows = db.read(table='document', query={'id': document_id}) or []
    if not document_rows:
        raise ValueError(f'Document not found: {document_id}')

    document_row = document_rows[0]

    try:
        output_text, provider, assessment = _extract_document_text(
            document_row,
            normalized_language,
        )
        record_id = upsert_document_text_extraction_record(
            document_id=document_id,
            source_language=normalized_language,
            status='completed',
            output_text=output_text,
            provider=provider,
            error=None,
        )
        return {
            'id': record_id,
            'status': 'completed',
            'quality': assessment.as_dict(),
        }
    except Exception as exc:
        logger.warning(f'Failed text extraction for document {document_id}: {exc}')
        record_id = upsert_document_text_extraction_record(
            document_id=document_id,
            source_language=normalized_language,
            status='failed',
            output_text=None,
            provider=None,
            error=str(exc),
        )
        return {'id': record_id, 'status': 'failed', 'error': str(exc)}
