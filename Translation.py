from __future__ import annotations

import argparse
import html
import math
import os
import re
import shutil
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Protocol, Sequence
from urllib.parse import quote

import google.auth
import pymupdf
import torch
from dotenv import load_dotenv
from google.auth.exceptions import GoogleAuthError
from google.auth.transport.requests import AuthorizedSession
from transformers import AutoModelForSeq2SeqLM, AutoTokenizer
from transformers.tokenization_utils_base import PreTrainedTokenizerBase

load_dotenv(Path(__file__).resolve().with_name(".env"), override=False)

from src.components.clients.document_processing import (
    DocumentOCRPage,
    DocumentOCRRegion,
    extract_document_ocr,
)

PREVIEW_LENGTH = 300
DEFAULT_MODEL = "Helsinki-NLP/opus-mt-es-en"
TRANSLATION_MODEL_CACHE_DIRECTORY = Path(
    os.getenv(
        "TRANSLATION_MODEL_CACHE_DIRECTORY",
        Path(__file__).resolve().parent / ".cache" / "translation" / "models",
    )
).expanduser()
DEFAULT_MAX_INPUT_TOKENS = 400
DEFAULT_BATCH_SIZE = 8
DEFAULT_MIN_OCR_CONFIDENCE = 0.50
DEFAULT_GOOGLE_TRANSLATION_LOCATION = "global"
DEFAULT_GOOGLE_MAX_INPUT_CHARACTERS = 3_000
GOOGLE_TRANSLATE_REQUEST_CHARACTER_LIMIT = 30_000
GOOGLE_TRANSLATE_REQUEST_ITEM_LIMIT = 1_024
GOOGLE_CLOUD_PLATFORM_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
PRESERVED_GLOSSARY_PATTERN = re.compile(r"\b(?:Lcda|Lcdo)\.?", re.IGNORECASE)
PASSPORT_DOCUMENT_TYPES = {"passport", "pasaporte"}


@dataclass(slots=True)
class DocumentBlock:
    page_number: int
    source_text: str
    translated_text: str = ""


@dataclass(slots=True)
class TranslationChunk:
    block_index: int
    chunk_index: int
    source_text: str
    translated_text: str = ""


@dataclass(slots=True)
class OCRRegion:
    """An OCR paragraph with its original page coordinates."""

    page_number: int
    polygon: list[list[float]]
    source_text: str
    confidence: float | None = None
    page_width: float | None = None
    page_height: float | None = None
    render_scale: float | None = None
    page_rotation: int = 0
    region_kind: str = "paragraph"
    translated_text: str = ""
    skip_reason: str | None = None
    pdf_rect_override: tuple[float, float, float, float] | None = None
    background_color_override: tuple[float, float, float] | None = None
    text_alignment: int = 0


@dataclass(slots=True)
class RenderQualityReport:
    """Non-visual safeguards recorded for one positioned-PDF export."""

    translated_regions: int = 0
    preserved_invalid_coordinates: int = 0
    preserved_oversized_region: int = 0
    preserved_does_not_fit: int = 0


@dataclass(slots=True)
class VisualRegionClassification:
    """A page region that must retain its original visual presentation."""

    role: str
    rect: pymupdf.Rect
    source: str


class TranslationError(RuntimeError):
    pass


class TranslationEngine(Protocol):
    def translate_batch(self, texts: Sequence[str]) -> list[str]:
        ...

    def split_text(self, text: str, maximum_chunk_size: int) -> list[str]:
        ...

    def join_translated_chunks(self, chunks: Sequence[str]) -> str:
        ...


class MarianTransformersEngine:
    """Spanish-to-English MarianMT inference loaded once per script run."""

    def __init__(self, model_name: str, device: str, max_input_tokens: int):
        self._tokenizer = AutoTokenizer.from_pretrained(
            model_name,
            cache_dir=str(TRANSLATION_MODEL_CACHE_DIRECTORY),
        )
        self._model = AutoModelForSeq2SeqLM.from_pretrained(
            model_name,
            cache_dir=str(TRANSLATION_MODEL_CACHE_DIRECTORY),
        )
        self._model.to(device)
        self._model.eval()
        self._device = device
        self._max_input_tokens = max_input_tokens

    def translate_batch(self, texts: Sequence[str]) -> list[str]:
        encoded = self._tokenizer(
            list(texts),
            return_tensors="pt",
            padding=True,
            truncation=False,
        )
        input_lengths = encoded["attention_mask"].sum(dim=1).tolist()
        if any(length > self._max_input_tokens for length in input_lengths):
            raise TranslationError(
                "A translation chunk exceeded the configured token limit."
            )

        encoded = {key: value.to(self._device) for key, value in encoded.items()}
        with torch.inference_mode():
            generated = self._model.generate(
                **encoded,
                num_beams=1,
                max_new_tokens=512,
            )

        return self._tokenizer.batch_decode(
            generated,
            skip_special_tokens=True,
            clean_up_tokenization_spaces=True,
        )

    def split_text(self, text: str, maximum_chunk_size: int) -> list[str]:
        return create_token_safe_chunks(text, self._tokenizer, maximum_chunk_size)

    def join_translated_chunks(self, chunks: Sequence[str]) -> str:
        return " ".join(chunks).strip()


class GoogleCloudTranslationEngine:
    """Cloud Translation Advanced NMT through the authenticated v3 REST API."""

    def __init__(
        self,
        project_id: str,
        location: str,
        max_input_characters: int,
    ) -> None:
        if not project_id:
            raise TranslationError(
                "Google Cloud Translation requires GOOGLE_TRANSLATE_PROJECT_ID, "
                "GOOGLE_CLOUD_PROJECT, or GOOGLE_DOCUMENT_AI_PROJECT_ID."
            )
        if not location:
            raise TranslationError("Google Cloud Translation location cannot be empty.")
        if not 1 <= max_input_characters <= GOOGLE_TRANSLATE_REQUEST_CHARACTER_LIMIT:
            raise TranslationError(
                "google_max_input_characters must be between 1 and "
                f"{GOOGLE_TRANSLATE_REQUEST_CHARACTER_LIMIT}."
            )

        try:
            credentials, _ = google.auth.default(scopes=[GOOGLE_CLOUD_PLATFORM_SCOPE])
            if hasattr(credentials, "with_quota_project"):
                credentials = credentials.with_quota_project(project_id)
        except GoogleAuthError as exc:
            raise TranslationError(
                "Unable to load Google application credentials for Cloud Translation."
            ) from exc
        self._session = AuthorizedSession(credentials)
        self._project_id = project_id
        self._location = location
        self._max_input_characters = max_input_characters
        self._endpoint = (
            "https://translation.googleapis.com/v3/projects/"
            f"{quote(project_id, safe='')}/locations/{quote(location, safe='')}:translateText"
        )

    def split_text(self, text: str, maximum_chunk_size: int) -> list[str]:
        # Cloud Translation recommends requests below 30,000 code points. Keep
        # individual regions smaller so a normal batch stays under that limit.
        return create_character_safe_chunks(
            text,
            min(maximum_chunk_size, self._max_input_characters),
        )

    def translate_batch(self, texts: Sequence[str]) -> list[str]:
        layout_texts = [preserve_translation_layout(text) for text in texts]
        if not layout_texts:
            return []
        if len(layout_texts) > GOOGLE_TRANSLATE_REQUEST_ITEM_LIMIT:
            raise TranslationError("Google Translation batch contains too many items.")
        total_characters = sum(len(text) for text in layout_texts)
        if total_characters > GOOGLE_TRANSLATE_REQUEST_CHARACTER_LIMIT:
            raise TranslationError(
                "Google Translation batch exceeds the 30,000-character request limit."
            )

        try:
            response = self._session.post(
                self._endpoint,
                json={
                    "contents": layout_texts,
                    "mimeType": "text/plain",
                    "sourceLanguageCode": "es",
                    "targetLanguageCode": "en",
                    "model": (
                        f"projects/{self._project_id}/locations/{self._location}"
                        "/models/general/nmt"
                    ),
                },
                headers={"x-goog-user-project": self._project_id},
                timeout=60,
            )
        except GoogleAuthError as exc:
            raise TranslationError(
                "Google Cloud Translation authentication or network request failed."
            ) from exc
        if not response.ok:
            detail = normalize_translation_text(response.text)[:500]
            raise TranslationError(
                "Google Cloud Translation request failed "
                f"(HTTP {response.status_code}): {detail or '<no error detail>'}"
            )
        try:
            translations = response.json()["translations"]
            translated_texts = [
                html.unescape(str(item["translatedText"])) for item in translations
            ]
        except (KeyError, TypeError, ValueError) as exc:
            raise TranslationError(
                "Google Cloud Translation returned an unexpected response."
            ) from exc
        if len(translated_texts) != len(layout_texts):
            raise TranslationError(
                "Google Cloud Translation returned a different number of "
                "translations than inputs."
            )
        return translated_texts

    def join_translated_chunks(self, chunks: Sequence[str]) -> str:
        # The API retains newlines for text/plain. Preserve them between
        # character-safe chunks too, rather than turning a document into one
        # paragraph after translation.
        return "\n".join(chunks).strip()


def preview_text(value: str) -> str:
    normalized = (value or "").strip()
    if not normalized:
        return "<empty>"
    return normalized[:PREVIEW_LENGTH]


def postprocess_translation(text: str) -> str:
    """Normalize a model response without applying document-specific wording."""

    return preserve_translation_layout(text)


def normalized_document_type(value: str | None) -> str:
    return re.sub(r"[^a-z]+", " ", str(value or "").lower()).strip()


def is_passport_document(
    input_path: Path,
    document_type: str | None = None,
) -> bool:
    """Classify passports without invoking OCR.

    Explicit upstream metadata has priority. If it is absent, the filename and
    native PDF metadata provide a conservative fallback. Image content is never
    inspected because that would violate the pre-OCR bypass requirement.
    """

    if document_type is not None:
        normalized_type = normalized_document_type(document_type)
        return any(
            re.search(rf"\b{re.escape(value)}\b", normalized_type)
            for value in PASSPORT_DOCUMENT_TYPES
        )

    normalized_name = normalized_document_type(input_path.stem)
    if any(
        re.search(rf"\b{re.escape(value)}\b", normalized_name)
        for value in PASSPORT_DOCUMENT_TYPES
    ):
        return True
    try:
        with pymupdf.open(input_path) as document:
            metadata = document.metadata or {}
        native_metadata = normalized_document_type(
            " ".join(
                str(metadata.get(key) or "")
                for key in ("title", "subject", "keywords")
            )
        )
    except (OSError, RuntimeError, ValueError):
        return False
    return any(
        re.search(rf"\b{re.escape(value)}\b", native_metadata)
        for value in PASSPORT_DOCUMENT_TYPES
    )


def copy_passport_without_translation(input_path: Path, output_path: Path) -> None:
    """Copy a passport unchanged, before OCR or translation clients are built."""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(input_path, output_path)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "OCR a scanned Spanish PDF, translate it to English with Google "
            "Cloud Translation or MarianMT, "
            "and preserve the original page layout in the output PDF."
        )
    )
    parser.add_argument("input_pdf", type=Path, help="Source Spanish PDF")
    parser.add_argument("output_pdf", type=Path, help="Translated English PDF")
    parser.add_argument(
        "--document-type",
        help=(
            "Upstream document classification. Use 'passport' to bypass OCR "
            "and copy the PDF unchanged."
        ),
    )
    parser.add_argument(
        "--debug-overlay-pdf",
        type=Path,
        help=(
            "Optional PDF showing calibrated OCR boxes and IDs before "
            "translation rendering."
        ),
    )
    parser.add_argument(
        "--engine",
        choices=("google", "marian"),
        default="google",
        help="Translation engine (default: %(default)s).",
    )
    parser.add_argument(
        "--google-project-id",
        default=(
            os.getenv("GOOGLE_TRANSLATE_PROJECT_ID")
            or os.getenv("GOOGLE_CLOUD_PROJECT")
            or os.getenv("GOOGLE_DOCUMENT_AI_PROJECT_ID")
        ),
        help=(
            "Google Cloud project. Defaults to GOOGLE_TRANSLATE_PROJECT_ID, "
            "GOOGLE_CLOUD_PROJECT, or GOOGLE_DOCUMENT_AI_PROJECT_ID."
        ),
    )
    parser.add_argument(
        "--google-location",
        default=os.getenv(
            "GOOGLE_TRANSLATE_LOCATION", DEFAULT_GOOGLE_TRANSLATION_LOCATION
        ),
        help="Google Cloud Translation location (default: %(default)s).",
    )
    parser.add_argument(
        "--google-max-input-characters",
        type=int,
        default=DEFAULT_GOOGLE_MAX_INPUT_CHARACTERS,
        help=(
            "Maximum characters per Google translation item; batches remain "
            "below Google Cloud's 30,000-character request recommendation."
        ),
    )
    parser.add_argument(
        "--model",
        default=DEFAULT_MODEL,
        help="MarianMT model name; used only with --engine marian.",
    )
    parser.add_argument(
        "--device",
        choices=("auto", "cpu", "cuda", "mps"),
        default="auto",
    )
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument(
        "--ocr-min-confidence",
        type=float,
        default=DEFAULT_MIN_OCR_CONFIDENCE,
        help=(
            "Keep the original scan for OCR regions below this confidence "
            "score (default: %(default)s)."
        ),
    )
    parser.add_argument(
        "--max-input-tokens",
        type=int,
        default=DEFAULT_MAX_INPUT_TOKENS,
        help="MarianMT maximum source tokens per request item.",
    )
    return parser.parse_args()


def select_device(requested_device: str) -> str:
    if requested_device == "auto":
        if torch.cuda.is_available():
            return "cuda"
        if hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
            return "mps"
        return "cpu"

    if requested_device == "cuda" and not torch.cuda.is_available():
        raise TranslationError("CUDA was requested but is not available.")
    if requested_device == "mps":
        mps_available = (
            hasattr(torch.backends, "mps") and torch.backends.mps.is_available()
        )
        if not mps_available:
            raise TranslationError("MPS was requested but is not available.")
    return requested_device


def normalize_translation_text(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def preserve_translation_layout(text: str) -> str:
    """Normalize line endings without removing OCR paragraph structure."""

    return str(text or "").replace("\r\n", "\n").replace("\r", "\n").strip()


def clean_translation_source_text(text: str) -> str:
    """Apply only document-neutral whitespace cleanup to OCR text."""

    lines = [
        normalize_translation_text(line)
        for line in preserve_translation_layout(text).splitlines()
    ]
    return "\n".join(line for line in lines if line)


def is_sparse_oversized_ocr_artifact(region: OCRRegion) -> bool:
    """Reject tiny OCR guesses attached to a large decorative image box."""

    if not region.page_height or region.page_height <= 0:
        return False
    compact = re.sub(r"[^A-Za-zÀ-ÿ0-9]", "", region.source_text)
    if len(compact) > 3:
        return False
    _, top, _, bottom = polygon_bounds(region.polygon)
    return (bottom - top) / region.page_height >= 0.04


def is_non_latin_ocr_artifact(text: str) -> bool:
    """Keep isolated non-Latin OCR noise out of the Marian translation batch."""

    normalized = normalize_translation_text(text)
    return (
        1 <= len(normalized) <= 40
        and not re.search(r"[A-Za-zÀ-ÿ0-9]", normalized)
    )


def is_security_or_machine_text(text: str) -> bool:
    """Keep pure machine values untouched without hiding translatable labels.

    A region with a meaningful word label is safe to send to translation.
    URLs, timestamps, numeric-only values, and code-like identifiers are not.
    """

    normalized = normalize_translation_text(text).lower()
    compact = re.sub(r"\s+", "", normalized)
    alphabetic_words = re.findall(r"[a-zà-ÿ]+", normalized)
    if (
        re.search(r"(?:https?://|www\.|@|\bqr\b)", normalized)
        and len(alphabetic_words) <= 8
    ):
        return True
    if not alphabetic_words and any(character.isdigit() for character in compact):
        return True
    looks_machine_formatted = bool(
        len(alphabetic_words) <= 2
        and len(compact) >= 8
        and any(character.isdigit() for character in compact)
        and re.fullmatch(r"[a-z0-9/_\-.:]+", compact)
    )
    if not looks_machine_formatted:
        return False

    # No words (dates, timestamps, numeric table values) or a token-sized
    # prefix embedded in an identifier is machine data.  Longer words are
    # labels and must be translated while the model preserves their value.
    label_length = sum(len(word) for word in alphabetic_words)
    return not alphabetic_words or label_length < 5


def is_dense_structured_record(text: str) -> bool:
    """Detect form/table-like text using punctuation and value density."""

    normalized = normalize_translation_text(text)
    if not normalized:
        return False
    words = re.findall(r"[A-Za-zÀ-ÿ]+", normalized)
    digit_groups = re.findall(r"\d+[\d./:-]*", normalized)
    label_delimiters = len(re.findall(r"\S\s*[:=]", normalized))
    separators = len(re.findall(r"(?:\s{2,}|\||\t)", str(text or "")))
    lines = [line for line in str(text or "").splitlines() if line.strip()]
    value_density = len(digit_groups) / max(1, len(words) + len(digit_groups))
    return (
        label_delimiters >= 2
        or separators >= 2
        or (len(lines) >= 3 and value_density >= 0.25)
        or (len(normalized) >= 140 and value_density >= 0.38)
    )


def is_compact_data_row_geometry(
    text: str,
    polygon: Sequence[Sequence[float]],
) -> bool:
    """Detect a short, wide OCR row that mixes labels with identifiers."""

    left, top, right, bottom = polygon_bounds(polygon)
    width = max(1, right - left)
    height = max(1, bottom - top)
    has_identifier = bool(re.search(r"\d{4,}|\d{1,2}[-/]\w+[-/]\d{2,4}", text))
    return (
        has_identifier
        and width / height >= 8
        and len(re.findall(r"[A-Za-zÀ-ÿ]+", text)) >= 6
    )


def low_text_quality(text: str) -> bool:
    """Catch visibly corrupted OCR even when the engine reports confidence."""

    normalized = normalize_translation_text(text)
    words = re.findall(r"[A-Za-zÀ-ÿ]+", normalized)
    if len(words) < 4:
        return False
    malformed = sum(
        len(word) >= 18
        or bool(re.search(r"[A-ZÁÉÍÓÚÜÑ]{2,}[a-záéíóúüñ][A-ZÁÉÍÓÚÜÑ]", word))
        for word in words
    )
    return malformed / len(words) > 0.20


def region_skip_reason(region: OCRRegion, minimum_confidence: float) -> str | None:
    """Return why the source scan should be retained instead of translated."""

    # Some providers use 0.0 when paragraph confidence is unavailable. Those
    # values are normalized to None during extraction; real low scores remain
    # useful, document-neutral evidence that an overlay would be unsafe.
    if region.confidence is not None and region.confidence < minimum_confidence:
        return "low_ocr_confidence"
    if is_non_latin_ocr_artifact(region.source_text):
        return "non_latin_ocr_artifact"
    if is_sparse_oversized_ocr_artifact(region):
        return "corrupted_ocr_text"
    if is_security_or_machine_text(region.source_text):
        return "security_or_machine_text"
    if low_text_quality(region.source_text):
        return "corrupted_ocr_text"
    return None


def preserve_digital_signature_metadata(regions: Sequence[OCRRegion]) -> None:
    """Keep digital-signature stamps intact instead of translating over them."""

    marker_pattern = re.compile(
        r"\b(?:digitally signed by|firmado digitalmente por|firma digital)\b",
        re.IGNORECASE,
    )
    for marker in regions:
        if not marker_pattern.search(marker.source_text):
            continue
        left, top, right, bottom = polygon_bounds(marker.polygon)
        horizontal_margin = max(8.0, (marker.page_width or 0) * 0.05)
        vertical_margin = max(8.0, (marker.page_height or 0) * 0.04)
        for candidate in regions:
            if candidate.page_number != marker.page_number:
                continue
            center_x, center_y = region_center(candidate)
            if (
                left - horizontal_margin <= center_x <= right + horizontal_margin
                and top - vertical_margin <= center_y <= bottom + vertical_margin
            ):
                candidate.skip_reason = "digital_signature_metadata"


def extracted_text_to_blocks(output_text: str) -> list[DocumentBlock]:
    """Convert the OCR extractor's blank-line page separators into blocks."""

    page_texts = re.split(r"\n\s*\n+", str(output_text or "").strip())
    blocks = [
        DocumentBlock(page_number=index, source_text=normalize_translation_text(text))
        for index, text in enumerate(page_texts, start=1)
        if normalize_translation_text(text)
    ]
    if not blocks:
        raise TranslationError("OCR completed but no text is available to translate.")
    return blocks


def split_into_sentences(text: str) -> list[str]:
    pieces = re.split(r"(?<=[.!?])\s+(?=[A-Z0-9\"'“‘(])", text.strip())
    return [piece.strip() for piece in pieces if piece.strip()]


def token_count(text: str, tokenizer: PreTrainedTokenizerBase) -> int:
    return len(tokenizer.encode(text, add_special_tokens=False))


def split_oversized_text(
    text: str,
    tokenizer: PreTrainedTokenizerBase,
    max_tokens: int,
) -> list[str]:
    token_ids = tokenizer.encode(text, add_special_tokens=False)
    chunks = []
    # SentencePiece token slices can decode and then re-tokenize to a larger
    # sequence at artificial boundaries. Half-budget slices leave a reliable
    # safety margin and are validated before inference.
    slice_size = max(1, max_tokens // 2)
    for start in range(0, len(token_ids), slice_size):
        decoded = tokenizer.decode(
            token_ids[start : start + slice_size],
            skip_special_tokens=True,
        ).strip()
        if decoded:
            if token_count(decoded, tokenizer) > max_tokens:
                raise TranslationError(
                    "Unable to split OCR text within the model token limit."
                )
            chunks.append(decoded)
    return chunks


def create_token_safe_chunks(
    text: str,
    tokenizer: PreTrainedTokenizerBase,
    max_tokens: int,
) -> list[str]:
    if max_tokens < 32:
        raise ValueError("max_input_tokens must be at least 32.")

    output = []
    current_sentences: list[str] = []
    for sentence in split_into_sentences(text):
        if token_count(sentence, tokenizer) > max_tokens:
            if current_sentences:
                output.append(" ".join(current_sentences))
                current_sentences = []
            output.extend(split_oversized_text(sentence, tokenizer, max_tokens))
            continue

        candidate = " ".join([*current_sentences, sentence])
        if current_sentences and token_count(candidate, tokenizer) > max_tokens:
            output.append(" ".join(current_sentences))
            current_sentences = [sentence]
        else:
            current_sentences.append(sentence)

    if current_sentences:
        output.append(" ".join(current_sentences))
    return output


def create_character_safe_chunks(text: str, max_characters: int) -> list[str]:
    """Split text for Cloud Translation without flattening document layout."""

    if max_characters < 1:
        raise ValueError("maximum chunk size must be at least 1.")

    layout_text = preserve_translation_layout(text)
    if not layout_text:
        return []
    if len(layout_text) <= max_characters:
        return [layout_text]

    chunks: list[str] = []
    current = ""
    for line in layout_text.splitlines(keepends=True):
        candidate = line.rstrip("\n")
        line_break = "\n" if line.endswith("\n") else ""
        while len(candidate) > max_characters:
            split_at = candidate.rfind(" ", 0, max_characters + 1)
            if split_at <= 0:
                split_at = max_characters
            if current:
                chunks.append(current.rstrip())
                current = ""
            chunks.append(candidate[:split_at].strip())
            candidate = candidate[split_at:].strip()

        proposed = current + candidate + line_break
        if current and len(proposed) > max_characters:
            chunks.append(current.rstrip())
            current = candidate + line_break
        else:
            current = proposed
    if current:
        chunks.append(current.rstrip())
    return chunks


def build_translation_chunks(
    blocks: Sequence[DocumentBlock],
    engine: TranslationEngine,
    maximum_chunk_size: int,
) -> list[TranslationChunk]:
    chunks = []
    for block_index, block in enumerate(blocks):
        block_chunks = engine.split_text(block.source_text, maximum_chunk_size)
        for chunk_index, source_text in enumerate(block_chunks):
            chunks.append(
                TranslationChunk(
                    block_index=block_index,
                    chunk_index=chunk_index,
                    source_text=source_text,
                )
            )
    if not chunks:
        raise TranslationError("No translation chunks were created from OCR text.")
    return chunks


def batched(
    items: Sequence[TranslationChunk], batch_size: int
) -> Iterable[list[TranslationChunk]]:
    if batch_size < 1:
        raise ValueError("batch_size must be at least 1.")
    for start in range(0, len(items), batch_size):
        yield list(items[start : start + batch_size])


def translate_with_preserved_glossary(
    engine: TranslationEngine,
    texts: Sequence[str],
) -> list[str]:
    """Translate text fragments while keeping configured titles byte-for-byte."""

    if not any(PRESERVED_GLOSSARY_PATTERN.search(text) for text in texts):
        return engine.translate_batch(texts)

    plans: list[list[tuple[str, str | int, str, str]]] = []
    translatable_fragments: list[str] = []
    for text in texts:
        plan: list[tuple[str, str | int, str, str]] = []
        cursor = 0
        for match in PRESERVED_GLOSSARY_PATTERN.finditer(text):
            pieces = ((text[cursor : match.start()], False), (match.group(0), True))
            for piece, preserved in pieces:
                if not piece:
                    continue
                if preserved or not piece.strip():
                    plan.append(("fixed", piece, "", ""))
                    continue
                leading = piece[: len(piece) - len(piece.lstrip())]
                trailing = piece[len(piece.rstrip()) :]
                core = piece.strip()
                fragment_index = len(translatable_fragments)
                translatable_fragments.append(core)
                plan.append(("translated", fragment_index, leading, trailing))
            cursor = match.end()
        remainder = text[cursor:]
        if remainder:
            if remainder.strip():
                leading = remainder[: len(remainder) - len(remainder.lstrip())]
                trailing = remainder[len(remainder.rstrip()) :]
                fragment_index = len(translatable_fragments)
                translatable_fragments.append(remainder.strip())
                plan.append(("translated", fragment_index, leading, trailing))
            else:
                plan.append(("fixed", remainder, "", ""))
        plans.append(plan)

    translated_fragments = (
        engine.translate_batch(translatable_fragments)
        if translatable_fragments
        else []
    )
    if len(translated_fragments) != len(translatable_fragments):
        raise TranslationError(
            "The model returned a different number of glossary fragments than inputs."
        )

    results: list[str] = []
    for plan in plans:
        output: list[str] = []
        for kind, value, leading, trailing in plan:
            if kind == "fixed":
                output.append(str(value))
            else:
                output.append(
                    leading + translated_fragments[int(value)] + trailing
                )
        results.append("".join(output))
    return results


def translate_blocks(
    blocks: list[DocumentBlock],
    engine: TranslationEngine,
    batch_size: int,
    maximum_chunk_size: int,
) -> int:
    chunks = build_translation_chunks(blocks, engine, maximum_chunk_size)
    total_batches = (len(chunks) + batch_size - 1) // batch_size

    for batch_index, chunk_batch in enumerate(batched(chunks, batch_size), start=1):
        translations = translate_with_preserved_glossary(
            engine,
            [chunk.source_text for chunk in chunk_batch]
        )
        if len(translations) != len(chunk_batch):
            raise TranslationError(
                "The model returned a different number of translations than inputs."
            )

        for chunk, translated_text in zip(chunk_batch, translations, strict=True):
            translated_text = postprocess_translation(translated_text).strip()
            chunk.translated_text = translated_text
        print(f"translation_batch: {batch_index}/{total_batches}")

    grouped: dict[int, list[TranslationChunk]] = {}
    for chunk in chunks:
        grouped.setdefault(chunk.block_index, []).append(chunk)
    for block_index, block in enumerate(blocks):
        block_chunks = sorted(
            grouped.get(block_index, []), key=lambda item: item.chunk_index
        )
        if not block_chunks:
            raise TranslationError(f"Missing translation for block {block_index}.")
        if any(not chunk.translated_text for chunk in block_chunks):
            # A malformed OCR fragment can make Marian emit an empty string.
            # Leave its source scan visible instead of aborting the whole PDF.
            block.translated_text = ""
            continue
        block.translated_text = engine.join_translated_chunks(
            [chunk.translated_text for chunk in block_chunks]
        )
    return len(chunks)


def polygon_bounds(
    polygon: Sequence[Sequence[float]],
) -> tuple[float, float, float, float]:
    xs = [point[0] for point in polygon]
    ys = [point[1] for point in polygon]
    return min(xs), min(ys), max(xs), max(ys)


def polygon_reading_rotation(
    polygon: Sequence[Sequence[float]],
) -> int:
    """Return the cardinal reading rotation encoded by an OCR polygon."""

    if len(polygon) < 2:
        return 0
    delta_x = polygon[1][0] - polygon[0][0]
    delta_y = polygon[1][1] - polygon[0][1]
    if abs(delta_x) + abs(delta_y) < 1e-6:
        return 0
    angle = math.degrees(math.atan2(delta_y, delta_x)) % 360
    cardinal = int(round(angle / 90.0) * 90) % 360
    angular_error = abs((angle - cardinal + 180) % 360 - 180)
    return cardinal if angular_error <= 20 else 0


def effective_page_rotation(page) -> int:
    """Recover OCR normalization rotation when the provider reports zero.

    Document AI sometimes rotates a photographed ID for OCR but leaves the
    page rotation field at zero. Paragraph polygon vertex order still records
    that normalization. A strong page-level vote lets every line use the same
    coordinate transform, including line polygons that were made axis-aligned.
    """

    reported_rotation = int(page.rotation or 0) % 360
    if reported_rotation:
        return reported_rotation
    paragraph_rotations = [
        polygon_reading_rotation(region.polygon)
        for region in page.regions or []
        if len(region.polygon) >= 2
    ]
    if not paragraph_rotations:
        return 0
    votes = Counter(paragraph_rotations)
    reading_rotation, count = votes.most_common(1)[0]
    if reading_rotation == 0 or count / len(paragraph_rotations) < 0.70:
        return 0
    return (360 - reading_rotation) % 360


def region_center(region) -> tuple[float, float]:
    left, top, right, bottom = polygon_bounds(region.polygon)
    return (left + right) / 2, (top + bottom) / 2


def is_oversized_ocr_region(region, page_width: float, page_height: float) -> bool:
    """Route merged OCR paragraphs to line-level rendering instead."""

    if page_width <= 0 or page_height <= 0:
        return False
    left, top, right, bottom = polygon_bounds(region.polygon)
    region_width = max(0, right - left)
    region_height = max(0, bottom - top)
    return (
        region_height > page_height * 0.25
        or region_width * region_height > page_width * page_height * 0.30
    )


def line_belongs_to_region(line, region) -> bool:
    """Return whether an OCR line is inside a paragraph's geometry."""

    left, top, right, bottom = polygon_bounds(region.polygon)
    center_x, center_y = region_center(line)
    return left - 4 <= center_x <= right + 4 and top - 4 <= center_y <= bottom + 4


def lines_have_parallel_columns(lines, page_width: float) -> bool:
    """Return whether OCR lines occupy separate columns on the same row."""

    if page_width <= 0:
        return False
    bounds = [polygon_bounds(line.polygon) for line in lines]
    for index, first in enumerate(bounds):
        first_height = max(1.0, first[3] - first[1])
        for second in bounds[index + 1 :]:
            second_height = max(1.0, second[3] - second[1])
            vertical_overlap = max(
                0.0,
                min(first[3], second[3]) - max(first[1], second[1]),
            )
            horizontal_gap = max(first[0], second[0]) - min(first[2], second[2])
            if (
                vertical_overlap / min(first_height, second_height) >= 0.45
                and horizontal_gap >= page_width * 0.01
            ):
                return True
    return False


def is_prose_paragraph(paragraph, contained_lines, page_width: float) -> bool:
    """Identify narrative text that benefits from paragraph-level translation."""

    text = normalize_translation_text(paragraph.text)
    words = re.findall(r"[A-Za-zÀ-ÿ]+", text)
    digit_groups = re.findall(r"\d+[\d./:-]*", text)
    if len(words) < 16 or len(contained_lines) < 2:
        return False
    if lines_have_parallel_columns(contained_lines, page_width):
        return False
    if len(digit_groups) / max(1, len(words) + len(digit_groups)) > 0.20:
        return False
    if len(re.findall(r"\S\s*[:=]", text)) >= 2:
        return False
    average_line_characters = sum(
        len(normalize_translation_text(line.text)) for line in contained_lines
    ) / len(contained_lines)
    return average_line_characters >= 22


def split_line_into_table_cells(line, words) -> list[DocumentOCRRegion]:
    """Split an OCR line when large horizontal gaps reveal separate cells."""

    line_left, line_top, line_right, line_bottom = polygon_bounds(line.polygon)
    line_height = max(1.0, line_bottom - line_top)
    line_width = max(1.0, line_right - line_left)
    if line_width / line_height < 7:
        return []

    contained_words = []
    for word in words or []:
        left, top, right, bottom = polygon_bounds(word.polygon)
        vertical_overlap = max(0.0, min(line_bottom, bottom) - max(line_top, top))
        word_height = max(1.0, bottom - top)
        center_x, _ = region_center(word)
        if (
            vertical_overlap / min(line_height, word_height) >= 0.55
            and line_left - 3 <= center_x <= line_right + 3
        ):
            contained_words.append(word)
    contained_words.sort(key=lambda word: polygon_bounds(word.polygon)[0])
    if len(contained_words) < 2:
        return []

    word_heights = [
        max(1.0, polygon_bounds(word.polygon)[3] - polygon_bounds(word.polygon)[1])
        for word in contained_words
    ]
    median_height = sorted(word_heights)[len(word_heights) // 2]
    if line_height > median_height * 2.2:
        return []
    gap_threshold = max(9.0, median_height * 0.85)
    groups: list[list[object]] = [[contained_words[0]]]
    for word in contained_words[1:]:
        previous_right = polygon_bounds(groups[-1][-1].polygon)[2]
        current_left = polygon_bounds(word.polygon)[0]
        if current_left - previous_right >= gap_threshold:
            groups.append([word])
        else:
            groups[-1].append(word)
    if len(groups) < 2:
        return []

    table_cells: list[DocumentOCRRegion] = []
    for group in groups:
        bounds = [polygon_bounds(word.polygon) for word in group]
        left = min(value[0] for value in bounds)
        top = min(value[1] for value in bounds)
        right = max(value[2] for value in bounds)
        bottom = max(value[3] for value in bounds)
        text = " ".join(normalize_translation_text(word.text) for word in group)
        confidences = [
            word.confidence for word in group if word.confidence is not None
        ]
        table_cells.append(
            DocumentOCRRegion(
                polygon=[[left, top], [right, top], [right, bottom], [left, bottom]],
                text=text,
                confidence=(min(confidences) if confidences else line.confidence),
            )
        )
    return table_cells


def layout_regions_for_page(page) -> list[tuple[object, str]]:
    """Choose OCR paragraphs or lines from geometry, independent of document type."""

    selected: list[tuple[object, str]] = []
    selected_keys: set[tuple[str, tuple[float, float, float, float]]] = set()

    def append_region(region, region_kind: str) -> None:
        bounds = tuple(round(value, 2) for value in polygon_bounds(region.polygon))
        key = (normalize_translation_text(region.text), bounds)
        if key in selected_keys:
            return
        selected_keys.add(key)
        selected.append((region, region_kind))

    def append_line(line) -> None:
        table_cells = split_line_into_table_cells(line, page.words)
        if table_cells:
            for cell in table_cells:
                append_region(cell, "table_cell")
        else:
            append_region(line, "line")

    paragraphs = list(page.regions or [])
    if not paragraphs:
        for line in page.lines or []:
            append_line(line)
        return selected

    for paragraph in paragraphs:
        paragraph_cells = split_line_into_table_cells(paragraph, page.words)
        if paragraph_cells:
            for cell in paragraph_cells:
                append_region(cell, "table_cell")
            continue
        contained_lines = [
            line for line in page.lines if line_belongs_to_region(line, paragraph)
        ]
        should_split_into_lines = (
            is_oversized_ocr_region(paragraph, page.width, page.height)
            or is_dense_structured_record(paragraph.text)
            or is_compact_data_row_geometry(paragraph.text, paragraph.polygon)
            or (
                len(contained_lines) >= 2
                and not is_prose_paragraph(
                    paragraph,
                    contained_lines,
                    page.width,
                )
            )
        )
        if should_split_into_lines and contained_lines:
            for line in contained_lines:
                append_line(line)
            continue
        append_region(paragraph, "paragraph")

    for line in page.lines or []:
        if not any(
            line_belongs_to_region(line, paragraph) for paragraph in paragraphs
        ):
            append_line(line)
    return selected


def extract_local_pdf_regions(
    input_path: Path,
) -> tuple[list[OCRRegion], str]:
    """OCR local pages as positioned paragraphs for layout-preserving export."""

    if not input_path.exists():
        raise FileNotFoundError(f"Input PDF does not exist: {input_path}")
    if input_path.suffix.lower() != ".pdf":
        raise ValueError("Local input must use the .pdf extension.")

    source_language = "es"
    document_bytes = input_path.read_bytes()
    print("ocr_engine: shared document OCR provider", flush=True)
    try:
        ocr_result = extract_document_ocr(
            document_bytes,
            source_language=source_language,
        )
    except ValueError as exc:
        raise TranslationError(str(exc)) from exc

    regions: list[OCRRegion] = []
    for page in ocr_result.pages:
        page_rotation = effective_page_rotation(page)
        if page_rotation != int(page.rotation or 0) % 360:
            print(
                f"ocr_page_rotation_inferred: page={page.page_number} "
                f"rotation={page_rotation}",
                flush=True,
            )
        page_regions = layout_regions_for_page(page)
        for region, region_kind in page_regions:
            source_text = clean_translation_source_text(region.text)
            if source_text:
                polygon = [list(point) for point in region.polygon]
                regions.append(
                    OCRRegion(
                        page_number=page.page_number,
                        polygon=polygon,
                        source_text=source_text,
                        confidence=(
                            region.confidence
                            if region.confidence is not None
                            and region.confidence > 0
                            else None
                        ),
                        page_width=page.width,
                        page_height=page.height,
                        render_scale=page.render_scale,
                        page_rotation=page_rotation,
                        region_kind=region_kind,
                    )
                )
        print(
            f"ocr_positioned_page_result: page={page.page_number} "
            f"paragraphs={len(page.regions)} lines={len(page.lines)} "
            f"selected_regions={len(page_regions)}",
            flush=True,
        )

    if not regions:
        raise TranslationError("PDF OCR completed but produced no positioned text.")
    return regions, ocr_result.provider


def translate_regions(
    regions: list[OCRRegion],
    engine: TranslationEngine,
    batch_size: int,
    maximum_chunk_size: int,
    minimum_ocr_confidence: float,
) -> int:
    """Translate OCR regions while preserving their page-coordinate mapping."""

    for region in regions:
        safety_reason = region_skip_reason(region, minimum_ocr_confidence)
        if safety_reason:
            region.skip_reason = safety_reason
    preserve_digital_signature_metadata(regions)

    translatable_regions = [
        region
        for region in regions
        if not region.translated_text and not region.skip_reason
    ]

    if not translatable_regions:
        return 0
    blocks = [
        DocumentBlock(page_number=region.page_number, source_text=region.source_text)
        for region in translatable_regions
    ]
    chunk_count = translate_blocks(
        blocks,
        engine=engine,
        batch_size=batch_size,
        maximum_chunk_size=maximum_chunk_size,
    )
    for region, block in zip(translatable_regions, blocks, strict=True):
        if block.translated_text:
            region.translated_text = block.translated_text
        else:
            region.skip_reason = "empty_model_translation"
    return chunk_count


def calibrated_pdf_rect(
    region: OCRRegion,
    source_page,
) -> pymupdf.Rect | None:
    """Map provider-native OCR coordinates to the actual PDF page rectangle."""

    if region.pdf_rect_override is not None:
        rect = pymupdf.Rect(region.pdf_rect_override) & source_page.rect
        return rect if rect.width >= 4 and rect.height >= 3 else None
    if not region.page_width or not region.page_height:
        return None
    if region.page_width <= 0 or region.page_height <= 0 or not region.polygon:
        return None

    def source_normalized_point(point: Sequence[float]) -> tuple[float, float]:
        x = point[0] / region.page_width
        y = point[1] / region.page_height
        rotation = region.page_rotation % 360
        if rotation == 0:
            return x, y
        if rotation == 90:
            return 1 - y, x
        if rotation == 180:
            return 1 - x, 1 - y
        if rotation == 270:
            return y, 1 - x
        raise ValueError(f"Unsupported OCR page rotation: {region.page_rotation}")

    normalized_points = [
        source_normalized_point(point) for point in region.polygon
    ]
    xs = [
        source_page.rect.x0 + point[0] * source_page.rect.width
        for point in normalized_points
    ]
    ys = [
        source_page.rect.y0 + point[1] * source_page.rect.height
        for point in normalized_points
    ]
    raw_rect = pymupdf.Rect(min(xs), min(ys), max(xs), max(ys))
    tolerance = 2.0
    if (
        raw_rect.x0 < source_page.rect.x0 - tolerance
        or raw_rect.y0 < source_page.rect.y0 - tolerance
        or raw_rect.x1 > source_page.rect.x1 + tolerance
        or raw_rect.y1 > source_page.rect.y1 + tolerance
    ):
        return None
    rect = raw_rect & source_page.rect
    if rect.width < 4 or rect.height < 3:
        return None
    expansion = 1.0 if region.region_kind == "table_cell" else 0.5
    expanded = pymupdf.Rect(
        rect.x0 - expansion,
        rect.y0 - expansion,
        rect.x1 + expansion,
        rect.y1 + expansion,
    )
    return expanded & source_page.rect


def looks_like_document_information(text: str) -> bool:
    """Return whether visual preservation would hide meaningful document data."""

    normalized = normalize_translation_text(text).lower()
    words = re.findall(r"[a-zà-ÿ]+", normalized)
    if any(character.isdigit() for character in normalized):
        return True
    if ":" in normalized or len(words) >= 12 or len(normalized) >= 100:
        return True
    information_terms = {
        "account",
        "address",
        "amount",
        "certification",
        "certificación",
        "date",
        "domicilio",
        "fecha",
        "identidad",
        "identity",
        "ingresos",
        "name",
        "nombre",
        "número",
        "numero",
        "statement",
    }
    return bool(information_terms.intersection(words))


def embedded_logo_rectangles(page) -> list[pymupdf.Rect]:
    """Find small embedded visual assets without treating a page scan as a logo."""

    page_area = max(1.0, page.rect.width * page.rect.height)
    logo_rectangles: list[pymupdf.Rect] = []
    seen: set[tuple[float, float, float, float]] = set()
    for image in page.get_images(full=True):
        xref = int(image[0])
        try:
            rectangles = page.get_image_rects(xref)
        except (RuntimeError, ValueError):
            continue
        for rectangle in rectangles:
            rect = pymupdf.Rect(rectangle) & page.rect
            area_ratio = rect.width * rect.height / page_area
            near_header_or_margin = (
                rect.y0 <= page.rect.y0 + page.rect.height * 0.30
                or rect.x0 <= page.rect.x0 + page.rect.width * 0.12
                or rect.x1 >= page.rect.x1 - page.rect.width * 0.12
            )
            if not (0.0002 <= area_ratio <= 0.12 and near_header_or_margin):
                continue
            key = tuple(round(value, 2) for value in rect)
            if key not in seen:
                seen.add(key)
                logo_rectangles.append(rect)
    return logo_rectangles


def joined_cell_text(
    members: Sequence[tuple[OCRRegion, pymupdf.Rect]],
) -> str:
    """Reconstruct one filled table cell while retaining its row structure."""

    ordered = sorted(members, key=lambda item: (item[1].y0, item[1].x0))
    lines: list[list[tuple[OCRRegion, pymupdf.Rect]]] = []
    for member in ordered:
        center_y = (member[1].y0 + member[1].y1) / 2
        if not lines:
            lines.append([member])
            continue
        previous_centers = [
            (item[1].y0 + item[1].y1) / 2 for item in lines[-1]
        ]
        previous_height = max(item[1].height for item in lines[-1])
        if abs(center_y - sum(previous_centers) / len(previous_centers)) <= max(
            1.5, previous_height * 0.55
        ):
            lines[-1].append(member)
        else:
            lines.append([member])
    return "\n".join(
        " ".join(
            item[0].source_text
            for item in sorted(line, key=lambda member: member[1].x0)
        )
        for line in lines
    )


def consolidate_filled_table_cells(
    input_path: Path,
    regions: Sequence[OCRRegion],
) -> tuple[list[OCRRegion], int]:
    """Merge OCR fragments into native filled cells for clean table rendering."""

    indexed_regions = list(enumerate(regions))
    assignments: dict[int, tuple[pymupdf.Rect, tuple[float, float, float]]] = {}
    with pymupdf.open(input_path) as document:
        for page_index, page in enumerate(document):
            page_regions = [
                (index, region, calibrated_pdf_rect(region, page))
                for index, region in indexed_regions
                if region.page_number == page_index + 1
            ]
            page_area = max(1.0, page.rect.width * page.rect.height)
            filled_cells: list[
                tuple[pymupdf.Rect, tuple[float, float, float]]
            ] = []
            for drawing in page.get_drawings():
                fill = drawing.get("fill")
                if fill is None:
                    continue
                rect = pymupdf.Rect(drawing["rect"]) & page.rect
                area_ratio = rect.width * rect.height / page_area
                if (
                    rect.width >= 10
                    and 5 <= rect.height <= page.rect.height * 0.08
                    and area_ratio <= 0.08
                ):
                    filled_cells.append(
                        (rect, tuple(float(channel) for channel in fill[:3]))
                    )
            for index, _region, region_rect in page_regions:
                if region_rect is None:
                    continue
                center = pymupdf.Point(
                    (region_rect.x0 + region_rect.x1) / 2,
                    (region_rect.y0 + region_rect.y1) / 2,
                )
                candidates = [
                    cell for cell in filled_cells if center in cell[0]
                ]
                if candidates:
                    assignments[index] = min(
                        candidates,
                        key=lambda cell: cell[0].width * cell[0].height,
                    )

    groups: dict[
        tuple[int, tuple[float, float, float, float]],
        list[tuple[int, OCRRegion, pymupdf.Rect]],
    ] = {}
    for index, (cell_rect, _fill) in assignments.items():
        region = regions[index]
        with pymupdf.open(input_path) as document:
            region_rect = calibrated_pdf_rect(
                region,
                document[region.page_number - 1],
            )
        if region_rect is None:
            continue
        key = (
            region.page_number,
            tuple(round(value, 3) for value in cell_rect),
        )
        groups.setdefault(key, []).append((index, region, region_rect))

    replacements: dict[int, OCRRegion] = {}
    removed_indices: set[int] = set()
    for members in groups.values():
        member_indices = [item[0] for item in members]
        first = min(members, key=lambda item: item[0])[1]
        cell_rect, fill = assignments[min(member_indices)]
        union = pymupdf.Rect(members[0][2])
        for _index, _region, member_rect in members[1:]:
            union |= member_rect
        alignment = (
            1
            if abs(
                (union.x0 + union.x1) / 2
                - (cell_rect.x0 + cell_rect.x1) / 2
            )
            <= cell_rect.width * 0.18
            else 0
        )
        inset = min(0.8, cell_rect.height * 0.08)
        merged = OCRRegion(
            page_number=first.page_number,
            polygon=[list(point) for point in first.polygon],
            source_text=joined_cell_text(
                [(region, rect) for _index, region, rect in members]
            ),
            confidence=min(
                (
                    region.confidence
                    for _index, region, _rect in members
                    if region.confidence is not None
                ),
                default=None,
            ),
            page_width=first.page_width,
            page_height=first.page_height,
            render_scale=first.render_scale,
            page_rotation=first.page_rotation,
            region_kind="table_header",
            pdf_rect_override=(
                cell_rect.x0 + inset,
                cell_rect.y0 + inset,
                cell_rect.x1 - inset,
                cell_rect.y1 - inset,
            ),
            background_color_override=fill,
            text_alignment=alignment,
        )
        replacement_index = min(member_indices)
        replacements[replacement_index] = merged
        removed_indices.update(member_indices)

    consolidated: list[OCRRegion] = []
    for index, region in indexed_regions:
        if index in replacements:
            consolidated.append(replacements[index])
        elif index not in removed_indices:
            consolidated.append(region)
    return consolidated, len(groups)


def classify_preserved_visual_regions(
    input_path: Path,
    regions: Sequence[OCRRegion],
) -> tuple[list[VisualRegionClassification], Counter]:
    """Protect header/logo visuals while allowing document information through.

    Protected regions are never redrawn, so the original PDF image and scaling
    remain byte-for-byte visually intact in those coordinates.
    """

    classifications: list[VisualRegionClassification] = []
    counts: Counter = Counter()
    regions_by_page: dict[int, list[OCRRegion]] = {}
    for region in regions:
        regions_by_page.setdefault(region.page_number, []).append(region)

    with pymupdf.open(input_path) as document:
        for page_index, page in enumerate(document):
            logo_rectangles = embedded_logo_rectangles(page)
            counts["logo_images_detected"] += len(logo_rectangles)
            for region in regions_by_page.get(page_index + 1, []):
                rect = calibrated_pdf_rect(region, page)
                if rect is None or looks_like_document_information(
                    region.source_text
                ):
                    continue
                reason: str | None = None
                region_area = max(1.0, rect.width * rect.height)
                for logo_rect in logo_rectangles:
                    intersection = rect & logo_rect
                    if intersection.width * intersection.height / region_area >= 0.25:
                        reason = "logo_visual"
                        break
                if reason is None:
                    top_limit = page.rect.y0 + page.rect.height * 0.16
                    word_count = len(re.findall(r"[A-Za-zÀ-ÿ]+", region.source_text))
                    if (
                        rect.y0 <= top_limit
                        and word_count <= 10
                        and len(normalize_translation_text(region.source_text)) <= 80
                    ):
                        reason = "header_visual"
                if reason is None:
                    continue
                if not region.skip_reason:
                    region.skip_reason = reason
                classifications.append(
                    VisualRegionClassification(
                        role=reason.removesuffix("_visual"),
                        rect=pymupdf.Rect(rect),
                        source=region.source_text,
                    )
                )
                counts[f"preserved_{reason}"] += 1
    return classifications, counts


def partition_overlapping_line_rects(
    prepared_regions: Sequence[tuple[OCRRegion, pymupdf.Rect]],
) -> list[tuple[OCRRegion, pymupdf.Rect]]:
    """Trim overlapping horizontal OCR rows at their shared midpoint."""

    adjusted = [
        (region, pymupdf.Rect(rect)) for region, rect in prepared_regions
    ]
    horizontal_lines = [
        index
        for index, (region, _rect) in enumerate(adjusted)
        if region.region_kind == "line" and region.page_rotation % 180 == 0
    ]
    horizontal_lines.sort(key=lambda index: adjusted[index][1].y0)
    for position, first_index in enumerate(horizontal_lines):
        first_region, first_rect = adjusted[first_index]
        for second_index in horizontal_lines[position + 1 :]:
            second_region, second_rect = adjusted[second_index]
            if second_rect.y0 >= first_rect.y1:
                break
            horizontal_overlap = max(
                0.0,
                min(first_rect.x1, second_rect.x1)
                - max(first_rect.x0, second_rect.x0),
            )
            if horizontal_overlap / max(
                1.0, min(first_rect.width, second_rect.width)
            ) < 0.55:
                continue
            if first_rect.y0 > second_rect.y0:
                continue
            boundary = (first_rect.y1 + second_rect.y0) / 2
            if boundary - first_rect.y0 >= 3:
                first_rect.y1 = boundary - 0.15
            if second_rect.y1 - boundary >= 3:
                second_rect.y0 = boundary + 0.15
            adjusted[first_index] = (first_region, first_rect)
            adjusted[second_index] = (second_region, second_rect)
    return adjusted


def source_line_count(region: OCRRegion) -> int:
    return max(1, len([line for line in region.source_text.splitlines() if line.strip()]))


def fit_translation_shape(
    page,
    region: OCRRegion,
    rect: pymupdf.Rect,
    text_color: tuple[float, float, float],
):
    """Return a fitted text shape, preserving the source when it cannot fit."""

    line_count = source_line_count(region)
    text_rotation = (360 - region.page_rotation) % 360
    layout_height = rect.width if text_rotation in {90, 270} else rect.height
    source_line_height = layout_height / max(1, line_count)
    maximum_font_size = min(18.0, max(6.0, source_line_height * 0.85))
    minimum_font_size = (
        2.5 if region.region_kind in {"line", "table_cell"} else 3.0
    )
    starting_font_size = min(
        maximum_font_size,
        max(minimum_font_size, source_line_height * 0.78),
    )
    step_count = int((starting_font_size - minimum_font_size) / 0.5)
    font_sizes = [starting_font_size - step * 0.5 for step in range(step_count + 1)]
    if font_sizes[-1] > minimum_font_size:
        font_sizes.append(minimum_font_size)
    for font_size in font_sizes:
        shape = page.new_shape()
        remaining = shape.insert_textbox(
            rect,
            region.translated_text,
            fontname="helv",
            fontsize=font_size,
            lineheight=1.02,
            color=text_color,
            rotate=text_rotation,
            align=region.text_alignment,
        )
        if remaining >= 0:
            return shape
    return None


def sampled_region_colors(
    source_page,
    source_pixmap,
    rect: pymupdf.Rect,
) -> tuple[tuple[float, float, float], tuple[float, float, float]]:
    """Estimate a region's background and choose readable translated text."""

    offset = 1.5
    sample_points = (
        (rect.x0 - offset, rect.y0 - offset),
        (rect.x1 + offset, rect.y0 - offset),
        (rect.x0 - offset, rect.y1 + offset),
        (rect.x1 + offset, rect.y1 + offset),
        ((rect.x0 + rect.x1) / 2, rect.y0 - offset),
        ((rect.x0 + rect.x1) / 2, rect.y1 + offset),
        (rect.x0 - offset, (rect.y0 + rect.y1) / 2),
        (rect.x1 + offset, (rect.y0 + rect.y1) / 2),
    )
    samples: list[tuple[int, int, int]] = []
    for x, y in sample_points:
        normalized_x = (x - source_page.rect.x0) / max(1.0, source_page.rect.width)
        normalized_y = (y - source_page.rect.y0) / max(1.0, source_page.rect.height)
        pixel_x = min(
            source_pixmap.width - 1,
            max(0, round(normalized_x * (source_pixmap.width - 1))),
        )
        pixel_y = min(
            source_pixmap.height - 1,
            max(0, round(normalized_y * (source_pixmap.height - 1))),
        )
        pixel = source_pixmap.pixel(pixel_x, pixel_y)
        samples.append((int(pixel[0]), int(pixel[1]), int(pixel[2])))

    median_rgb = tuple(
        sorted(sample[channel] for sample in samples)[len(samples) // 2]
        for channel in range(3)
    )
    background = tuple(channel / 255 for channel in median_rgb)
    luminance = (
        0.2126 * background[0]
        + 0.7152 * background[1]
        + 0.0722 * background[2]
    )
    text_color = (1.0, 1.0, 1.0) if luminance < 0.42 else (0.0, 0.0, 0.0)
    return background, text_color


def export_coordinate_debug_pdf(
    input_path: Path,
    regions: Sequence[OCRRegion],
    output_path: Path,
) -> None:
    """Write a visual calibration artifact before any text is translated."""

    if input_path.resolve() == output_path.resolve():
        raise ValueError("Debug overlay PDF must not overwrite the input PDF.")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists():
        output_path.unlink()
    document = pymupdf.open(input_path)
    regions_by_page: dict[int, list[OCRRegion]] = {}
    for region in regions:
        regions_by_page.setdefault(region.page_number, []).append(region)

    for page_index, page in enumerate(document):
        for index, region in enumerate(
            regions_by_page.get(page_index + 1, []),
            start=1,
        ):
            rect = calibrated_pdf_rect(region, page)
            if rect is None:
                continue
            color = (
                (0, 0.45, 0.95)
                if region.region_kind == "paragraph"
                else (0, 0.65, 0.15)
            )
            page.draw_rect(rect, color=color, width=0.6, overlay=True)
            page.insert_text(
                (rect.x0, max(7, rect.y0 + 7)),
                str(index),
                fontsize=6,
                color=color,
                overlay=True,
            )
    document.save(output_path, garbage=4, deflate=True)
    document.close()


def export_positioned_translation_pdf(
    input_path: Path,
    regions: Sequence[OCRRegion],
    output_path: Path,
) -> RenderQualityReport:
    """Overlay fitted English translations on calibrated source text regions."""

    if input_path.resolve() == output_path.resolve():
        raise ValueError("Translated PDF must not overwrite the input PDF.")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists():
        output_path.unlink()
    document = pymupdf.open(input_path)
    regions_by_page: dict[int, list[OCRRegion]] = {}
    for region in regions:
        regions_by_page.setdefault(region.page_number, []).append(region)
    report = RenderQualityReport()

    for page_index, page in enumerate(document):
        page_number = page_index + 1
        page_regions = regions_by_page.get(page_number, [])
        source_pixmap = page.get_pixmap(alpha=False)
        prepared_regions: list[tuple[OCRRegion, pymupdf.Rect]] = []
        for region in page_regions:
            if not region.translated_text:
                continue
            rect = calibrated_pdf_rect(region, page)
            if rect is None:
                region.skip_reason = "invalid_render_coordinates"
                region.translated_text = ""
                report.preserved_invalid_coordinates += 1
                continue
            prepared_regions.append((region, rect))

        for region, rect in partition_overlapping_line_rects(prepared_regions):
            if region.region_kind == "paragraph" and (
                rect.width * rect.height > page.rect.width * page.rect.height * 0.30
            ):
                region.skip_reason = "oversized_render_region"
                region.translated_text = ""
                report.preserved_oversized_region += 1
                continue
            background_color, text_color = sampled_region_colors(
                page,
                source_pixmap,
                rect,
            )
            fitted_shape = fit_translation_shape(page, region, rect, text_color)
            if fitted_shape is None:
                region.skip_reason = "translation_does_not_fit"
                region.translated_text = ""
                report.preserved_does_not_fit += 1
                continue
            page.draw_rect(rect, color=None, fill=background_color, overlay=True)
            fitted_shape.commit(overlay=True)
            report.translated_regions += 1

    document.save(output_path, garbage=4, deflate=True)
    document.close()
    return report


def run_local_pdf_translation(args: argparse.Namespace) -> None:
    if args.output_pdf.suffix.lower() != ".pdf":
        raise ValueError("Local output must use the .pdf extension.")
    if args.input_pdf.resolve() == args.output_pdf.resolve():
        raise ValueError("Input and output PDF paths must be different.")
    if is_passport_document(
        args.input_pdf,
        getattr(args, "document_type", None),
    ):
        copy_passport_without_translation(args.input_pdf, args.output_pdf)
        with pymupdf.open(args.input_pdf) as source_document:
            source_pages = source_document.page_count
        print("\n=== passport bypass summary ===")
        print("document_type: passport")
        print("ocr_executed: false")
        print("translation_executed: false")
        print(f"source_pages: {source_pages}")
        print(f"preserved_pdf: {args.output_pdf}")
        return

    regions, provider = extract_local_pdf_regions(args.input_pdf)
    visual_classifications, visual_counts = classify_preserved_visual_regions(
        args.input_pdf,
        regions,
    )
    with pymupdf.open(args.input_pdf) as source_document:
        source_pages = source_document.page_count
    total_characters = sum(len(region.source_text) for region in regions)

    print("\n=== local extraction summary ===")
    print(f"provider: {provider}")
    print(f"source_pages: {source_pages}")
    print(f"ocr_regions: {len(regions)}")
    print(f"output_chars: {total_characters}")
    print(f"visual_regions_classified: {len(visual_classifications)}")
    for classification, count in sorted(visual_counts.items()):
        print(f"{classification}: {count}")
    confidence_values = [
        region.confidence
        for region in regions
        if region.confidence is not None
    ]
    if confidence_values:
        print(
            "ocr_confidence: "
            f"min={min(confidence_values):.3f} "
            f"avg={sum(confidence_values) / len(confidence_values):.3f} "
            f"gate={args.ocr_min_confidence:.3f}"
        )
    print("region_samples:")
    for region in regions[:5]:
        print({
            "page": region.page_number,
            "kind": region.region_kind,
            "chars": len(region.source_text),
            "preview": preview_text(region.source_text),
        })
    if args.debug_overlay_pdf:
        export_coordinate_debug_pdf(
            args.input_pdf,
            regions,
            args.debug_overlay_pdf,
        )
        print(f"coordinate_debug_pdf: {args.debug_overlay_pdf}")

    if args.engine == "google":
        print("translation_engine: google_cloud_nmt")
        print(f"google_project_id: {args.google_project_id}")
        print(f"google_location: {args.google_location}")
        engine: TranslationEngine = GoogleCloudTranslationEngine(
            project_id=str(args.google_project_id or "").strip(),
            location=str(args.google_location or "").strip(),
            max_input_characters=args.google_max_input_characters,
        )
        maximum_chunk_size = args.google_max_input_characters
    else:
        device = select_device(args.device)
        print("translation_engine: marian")
        print(f"translation_device: {device}")
        print(f"loading_translation_model: {args.model}")
        engine = MarianTransformersEngine(
            model_name=args.model,
            device=device,
            max_input_tokens=args.max_input_tokens,
        )
        maximum_chunk_size = args.max_input_tokens
    chunk_count = translate_regions(
        regions,
        engine=engine,
        batch_size=args.batch_size,
        maximum_chunk_size=maximum_chunk_size,
        minimum_ocr_confidence=args.ocr_min_confidence,
    )
    render_report = export_positioned_translation_pdf(
        args.input_pdf,
        regions,
        args.output_pdf,
    )
    skipped_regions = Counter(
        region.skip_reason for region in regions if region.skip_reason
    )
    print("\n=== local translation summary ===")
    print(f"source_pages: {source_pages}")
    print(f"translation_chunks: {chunk_count}")
    translated_region_count = sum(
        bool(region.translated_text) for region in regions
    )
    print(f"translated_regions: {translated_region_count}")
    print(f"preserved_source_regions: {len(regions) - translated_region_count}")
    for reason, count in sorted(skipped_regions.items()):
        print(f"preserved_{reason}: {count}")
    preserved_samples = sorted(
        [
            region
            for region in regions
            if region.skip_reason
        ],
        key=lambda region: (
            region.skip_reason not in {
                "invalid_render_coordinates",
                "oversized_render_region",
                "translation_does_not_fit",
            },
            region.page_number,
        ),
    )[:10]
    if preserved_samples:
        print("preserved_region_samples:")
        for region in preserved_samples:
            print(
                {
                    "page": region.page_number,
                    "kind": region.region_kind,
                    "reason": region.skip_reason,
                    "source": preview_text(region.source_text),
                }
            )
    print("\n=== render quality summary ===")
    print(f"rendered_translations: {render_report.translated_regions}")
    print(
        "preserved_invalid_render_coordinates: "
        f"{render_report.preserved_invalid_coordinates}"
    )
    print(
        "preserved_oversized_render_region: "
        f"{render_report.preserved_oversized_region}"
    )
    print(
        "preserved_translation_does_not_fit: "
        f"{render_report.preserved_does_not_fit}"
    )
    print(f"translated_pdf: {args.output_pdf}")



def main() -> None:
    args = parse_arguments()
    if args.batch_size < 1:
        raise ValueError("batch_size must be at least 1.")
    if args.max_input_tokens < 32:
        raise ValueError("max_input_tokens must be at least 32.")
    if not (
        1
        <= args.google_max_input_characters
        <= GOOGLE_TRANSLATE_REQUEST_CHARACTER_LIMIT
    ):
        raise ValueError(
            "google_max_input_characters must be between 1 and "
            f"{GOOGLE_TRANSLATE_REQUEST_CHARACTER_LIMIT}."
        )
    if not 0 <= args.ocr_min_confidence <= 1:
        raise ValueError("ocr_min_confidence must be between 0 and 1.")
    run_local_pdf_translation(args)


if __name__ == "__main__":
    main()
