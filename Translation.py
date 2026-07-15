from __future__ import annotations

import argparse
import html
import os
import re
import unicodedata
from collections import Counter
from dataclasses import dataclass
from difflib import SequenceMatcher
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

from src.components.clients.document_processing import extract_document_ocr

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
TRANSLATION_SOURCE_TEXT_REPLACEMENTS = {
    "YNOTARIOS": "Y NOTARIOS",
    "ACUERDO DE CONCILIACION": "ACUERDO DE CONCILIACIÓN",
    "aqosto": "agosto",
    "deL": "del",
    "SQLIS": "SOLÍS",
    "ROCIQ": "ROCÍO",
    "Ias": "las",
    "Ivombre": "Nombre",
    "1º Apelido": "1º Apellido",
    "28 Apellido": "2º Apellido",
}
TRANSLATION_OUTPUT_REPLACEMENTS = (
    (r"\bSERVIDUMBRE\s+TRASLADADA\b", "TRANSFERRED EASEMENT"),
)
FIXED_REGION_TRANSLATIONS = {
    "E&V": "E&V",
    "ABOGADOS Y NOTARIOS": "LAWYERS AND NOTARIES",
    "E&V ABOGADOS Y NOTARIOS": "E&V LAWYERS AND NOTARIES",
    "ACUERDO DE CONCILIACIÓN:": "CONCILIATION AGREEMENT:",
    "CONCILIADOR": "CONCILIATOR",
    "IMPRIMIR": "PRINT",
    "REGRESAR": "BACK",
    "COMPRAR": "BUY",
    "IMPRIMIR REGRESAR": "PRINT    BACK",
    "IMPRIMIR REGRESAR COMPRAR": "PRINT    BACK    BUY",
    "SERVIDUMBRE TRASLADADA": "TRANSFERRED EASEMENT",
    "N.MOTOR:": "ENGINE NO.:",
    "ESTE CIVIL": "CIVIL",
    "ESE CIVIL": "CIVIL",
    "GISTRO": "REGISTRY",
    "HIJO/A DE:": "CHILD OF:",
    "HIJO/A DE": "CHILD OF:",
    "EL DÍA:": "DATE:",
}
STRUCTURED_FIELD_TRANSLATIONS = (
    (r"\bTOMO\b", "VOLUME"),
    (r"\bASIENTO\b", "ENTRY"),
    (r"\bSECUENCIA\b", "SEQUENCE"),
    (r"\bFECHA\b", "DATE"),
    (r"\bFOLIO\b", "FOLIO"),
    (r"\bCITAS\b", "REFERENCES"),
)
SPANISH_MONTH_ABBREVIATIONS = {
    "ENE": "JAN",
    "FEB": "FEB",
    "MAR": "MAR",
    "ABR": "APR",
    "MAY": "MAY",
    "JUN": "JUN",
    "JUL": "JUL",
    "AGO": "AUG",
    "SEP": "SEP",
    "OCT": "OCT",
    "NOV": "NOV",
    "DIC": "DEC",
}
OFFICIAL_SEAL_MARKERS = (
    "TRIBUNAL SUPREMO DE ELECCIONES",
    "REGISTRO CIVIL",
    "DIRECCION DE CERTIFICACIONES DIGITALES",
    "DIRECCIÓN DE CERTIFICACIONES DIGITALES",
    "CERTIFICACIONES DIGITALES",
)
ISSUER_BRANDING_MARKERS: tuple[str, ...] = ()
OFFICIAL_BRANDING_FRAGMENTS = {
    "TRIBUNAL SUPREMO DE ELECCIONES",
    "SISTEMA",
    "DE CERTIFICACIONES",
    "DIGITALES",
    "SISTEMA DE CERTIFICACIONES DIGITALES",
    "CDI",
    "ISE",
}


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
    region_kind: str = "paragraph"
    translated_text: str = ""
    skip_reason: str | None = None
    glossary_match: str | None = None
    glossary_translation: str | None = None
    glossary_allows_structured_region: bool = False


@dataclass(frozen=True, slots=True)
class GlossaryEntry:
    """A verified Spanish label and its approved English rendering."""

    source_text: str
    translated_text: str
    aliases: tuple[str, ...] = ()
    allow_structured_region: bool = False


@dataclass(slots=True)
class RenderQualityReport:
    """Non-visual safeguards recorded for one positioned-PDF export."""

    translated_regions: int = 0
    preserved_invalid_coordinates: int = 0
    preserved_protected_overlap: int = 0
    preserved_oversized_region: int = 0
    preserved_does_not_fit: int = 0
    protected_regions: int = 0


# Initial Costa Rican civil/registry glossary. Entries are deliberately short,
# high-confidence labels: they may repair uncertain OCR without guessing at a
# whole paragraph or a table cell that has not been structurally extracted.
DOCUMENT_GLOSSARY = (
    GlossaryEntry(
        "CERTIFICA",
        "CERTIFIES:",
        ("CERIFICA", "CERIIACA", "CERIIFICA", "CERlFICA"),
        True,
    ),
    GlossaryEntry(
        "CÉDULA DE IDENTIDAD",
        "IDENTITY CARD",
        ("CEDULA DE IDENTIDAD", "CEDULA DE IDEN TIDAD"),
    ),
    GlossaryEntry(
        "CONTRATO PRENDARIO",
        "CHATTEL MORTGAGE CONTRACT",
        ("CONTRATO PRENDARIO",),
    ),
    GlossaryEntry(
        "NO POSEE ANOTACIÓN(ES)",
        "NO ANNOTATIONS",
        ("NO POSCE ANOTACIÓN(ES)", "NO POSCE ANOTACION(ES)"),
    ),
    GlossaryEntry(
        "NO POSEE LEVANTAMIENTO(S)",
        "NO RELEASES RECORDED",
        (
            "NO POSEE LEVANIAMIENTO(S)",
            "NO POSCE LEVANIAMIENTO(S)",
            "NO POSCE LEVANTAMIENTO(S)",
        ),
    ),
    GlossaryEntry("TOMO", "VOLUME", allow_structured_region=True),
    GlossaryEntry("FOLIO", "FOLIO", allow_structured_region=True),
    GlossaryEntry("ASIENTO", "ENTRY", allow_structured_region=True),
    GlossaryEntry("CITA", "REFERENCE", allow_structured_region=True),
    GlossaryEntry("FECHA", "DATE", allow_structured_region=True),
    GlossaryEntry("TIPO", "TYPE", allow_structured_region=True),
    GlossaryEntry("SECUENCIA", "SEQUENCE", allow_structured_region=True),
    GlossaryEntry(
        "CITAS DE INSCRIPCIÓN",
        "REG. REFERENCES",
        ("CITAS DE INSCRIPCION", "CITAS DE INSCRIP"),
        True,
    ),
    GlossaryEntry(
        "SI POSEE GRAVAMEN(ES)",
        "HAS ENCUMBRANCE(S)",
        ("SI POSEE GRAVAMENES",),
        True,
    ),
    GlossaryEntry("GRAVAMEN(ES)", "ENCUMBRANCE(S)", allow_structured_region=True),
    GlossaryEntry(
        "DENUNCIA DE TRÁNSITO",
        "TRAFFIC COMPLAINT",
        ("DENUNCIA DE TRANSITO",),
        True,
    ),
)


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
    """Replace known Spanish phrases that the translation model preserved."""

    output = str(text or "")
    for pattern, replacement in TRANSLATION_OUTPUT_REPLACEMENTS:
        output = re.sub(pattern, replacement, output, flags=re.IGNORECASE)
    return output


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
    """Repair recurring scan artifacts before sending text to translation."""

    cleaned = str(text or "")
    cleaned = re.sub(r"_+", " ", cleaned)
    for incorrect, replacement in TRANSLATION_SOURCE_TEXT_REPLACEMENTS.items():
        cleaned = cleaned.replace(incorrect, replacement)
    return normalize_translation_text(cleaned)


def translate_structured_form_labels(text: str) -> str:
    """Translate form labels while preserving identifiers and values verbatim."""

    source = preserve_translation_layout(text)
    normalized = normalize_translation_text(source).upper()
    label_count = sum(
        bool(re.search(rf"\b{label}\b", normalized))
        for label in ("TOMO", "ASIENTO", "SECUENCIA", "FECHA", "FOLIO", "CITAS")
    )
    reference_row = bool(re.search(r"\bCITAS?\b\s*:\s*\d", normalized))
    if label_count < 2 and not reference_row:
        return ""

    translated = source
    for pattern, replacement in STRUCTURED_FIELD_TRANSLATIONS:
        translated = re.sub(pattern, replacement, translated, flags=re.IGNORECASE)
    return translated if translated != source else ""


def translate_issued_ui_region(text: str) -> str:
    """Translate a merged issuance line and its nearby web-control labels."""

    normalized = normalize_translation_text(text)
    match = re.search(
        r"\bEMITIDO(?:\s+EL|:)?\s+(.+?)\s+A\s+LAS\s+([0-9:]+)\s+HORAS\b",
        normalized,
        flags=re.IGNORECASE,
    )
    if not match:
        return ""
    controls = [
        translation
        for source, translation in (
            ("IMPRIMIR", "PRINT"),
            ("REGRESAR", "BACK"),
            ("COMPRAR", "BUY"),
        )
        if re.search(rf"\b{source}\b", normalized, flags=re.IGNORECASE)
    ]
    issued_line = f"ISSUED: {match.group(1).strip()} {match.group(2)}"
    return issued_line + (f"\n{'   '.join(controls)}" if controls else "")


def translate_machine_value(text: str) -> str:
    """Translate language-bearing date values without altering identifiers."""

    translated = re.sub(
        r"\bFECHA\s+DE\s+NACIMIENTO\s*:\s*(\d{2})\s*(\d{2})\s+(\d{4})\b",
        lambda match: (
            f"DATE OF BIRTH: {match.group(1)}/{match.group(2)}/{match.group(3)}"
        ),
        text,
        flags=re.IGNORECASE,
    )
    translated = re.sub(
        r"\bVENCIMIENTO\s*:\s*(\d{2})\s*(\d{2})\s+(\d{4})\b",
        lambda match: (
            f"EXPIRATION DATE: {match.group(1)}/{match.group(2)}/{match.group(3)}"
        ),
        translated,
        flags=re.IGNORECASE,
    )
    translated = re.sub(
        r"\bFECHA\s+DE\s+INSCRIPCI[ÓO]N\b",
        "REGISTRATION DATE",
        translated,
        flags=re.IGNORECASE,
    )
    translated = re.sub(
        r"\b(\d{1,2})-([A-Za-z]{3})-(\d{4})\b",
        lambda match: (
            f"{match.group(1)}-"
            f"{SPANISH_MONTH_ABBREVIATIONS.get(match.group(2).upper(), match.group(2))}-"
            f"{match.group(3)}"
        ),
        translated,
    )
    return translated if translated != text else ""


def glossary_key(text: str) -> str:
    """Compare OCR safely despite accents, spaces, and punctuation artifacts."""

    decomposed = unicodedata.normalize("NFKD", str(text or ""))
    without_accents = "".join(
        character for character in decomposed if not unicodedata.combining(character)
    )
    return re.sub(r"[^A-Z0-9]", "", without_accents.upper())


def glossary_candidates(entry: GlossaryEntry) -> tuple[str, ...]:
    return (entry.source_text, *entry.aliases)


def is_uncertain_ocr(region: OCRRegion, minimum_confidence: float) -> bool:
    return (
        region.confidence is None
        or region.confidence < minimum_confidence
        or low_text_quality(region.source_text)
    )


def match_glossary(
    region: OCRRegion,
    minimum_confidence: float,
) -> tuple[GlossaryEntry, str] | None:
    """Return a verified glossary match for a short, uncertain OCR label."""

    source_key = glossary_key(region.source_text)
    if not source_key or len(region.source_text) > 80:
        return None

    for entry in DOCUMENT_GLOSSARY:
        if source_key in {glossary_key(candidate) for candidate in glossary_candidates(entry)}:
            return entry, "exact"

    if not is_uncertain_ocr(region, minimum_confidence):
        return None

    best_match: tuple[float, GlossaryEntry] | None = None
    for entry in DOCUMENT_GLOSSARY:
        for candidate in glossary_candidates(entry):
            candidate_key = glossary_key(candidate)
            if len(candidate_key) < 6 or abs(len(source_key) - len(candidate_key)) > 3:
                continue
            similarity = SequenceMatcher(None, source_key, candidate_key).ratio()
            if best_match is None or similarity > best_match[0]:
                best_match = similarity, entry

    if best_match and best_match[0] >= 0.80:
        return best_match[1], "fuzzy"
    return None


def apply_glossary_context(
    regions: Sequence[OCRRegion],
    minimum_confidence: float,
) -> None:
    """Attach verified English labels before deciding whether OCR may be overlaid."""

    for region in regions:
        match = match_glossary(region, minimum_confidence)
        if match is None:
            continue
        entry, match_type = match
        region.glossary_match = f"{match_type}:{entry.source_text}"
        region.glossary_translation = entry.translated_text
        region.glossary_allows_structured_region = entry.allow_structured_region


def is_signature_initials(region: OCRRegion) -> bool:
    """Preserve footer initials and signature-like marks on scanned IDs."""

    compact = re.sub(r"[^A-Za-z]", "", str(region.source_text or ""))
    if region.page_height is None or region.page_height <= 0:
        return False
    if region.page_width is None or region.page_width <= 0:
        return False
    left, top, right, bottom = polygon_bounds(region.polygon)
    if (
        1 <= len(compact) <= 5
        and bottom >= region.page_height * 0.88
        and uppercase_ratio(compact) >= 0.70
    ):
        return True

    # Handwritten/card signature marks are sometimes recognized as a few
    # title-case words inside an unusually tall box. Translating that OCR
    # guess creates a conspicuous white rectangle over the identity card.
    words = re.findall(r"[A-Za-zÀ-ÿ]+", region.source_text)
    relative_width = max(0.0, right - left) / region.page_width
    relative_height = max(0.0, bottom - top) / region.page_height
    center_y = (top + bottom) / 2 / region.page_height
    return (
        2 <= len(words) <= 4
        and len(compact) <= 24
        and not re.search(r"\d|[:./]", region.source_text)
        and uppercase_ratio(region.source_text) < 0.60
        and relative_width >= 0.18
        and relative_height >= 0.035
        and 0.32 <= center_y <= 0.58
    )


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


def uppercase_ratio(text: str) -> float:
    letters = [character for character in str(text or "") if character.isalpha()]
    if not letters:
        return 0.0
    return sum(character.isupper() for character in letters) / len(letters)


def is_security_or_machine_text(text: str) -> bool:
    """Keep pure machine values untouched without hiding translatable labels.

    Short form fields such as ``MATRICULA: 86412-F-000`` and
    ``Peso Remolque: 0`` used to be classified as machine text because they
    contain few words and several digits.  That preserved the identifier, but
    it also left the Spanish label visible.  A region with a meaningful label
    is safe to send to translation; URLs and code-only values are not.
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


def is_issuer_branding(region: OCRRegion) -> bool:
    """Preserve known issuer branding without hiding ordinary margin labels."""

    normalized = normalize_translation_text(region.source_text).upper()
    if is_official_seal(region):
        return True
    if (
        normalized in OFFICIAL_BRANDING_FRAGMENTS
        and region.polygon
        and region.page_height
        and region.page_height > 0
    ):
        _, top, _, bottom = polygon_bounds(region.polygon)
        center_y = (top + bottom) / 2
        if 0.30 <= center_y / region.page_height <= 0.55:
            return True
    return any(marker in normalized for marker in ISSUER_BRANDING_MARKERS)


def is_official_seal(region: OCRRegion) -> bool:
    """Preserve text embedded in artwork, not ordinary government headings."""

    normalized = normalize_translation_text(region.source_text).upper()
    if region.polygon and region.page_height and region.page_height > 0:
        _, top, _, bottom = polygon_bounds(region.polygon)
        center_y = (top + bottom) / 2 / region.page_height
        exact_seal_fragment = normalized in {
            "REGISTRO",
            "GISTRO",
            "CIVIL",
            "REPUBLICA DE COSTA RICA",
            "REPÚBLICA DE COSTA RICA",
        }
        if exact_seal_fragment and 0.64 <= center_y <= 0.80:
            return True
        if (
            normalized in {"REPUBLICA DE COSTA RICA", "REPÚBLICA DE COSTA RICA"}
            and 0.25 <= center_y <= 0.40
        ):
            return True

    # Civil-registry seal fragments are returned as independent OCR lines.
    # Their lower-page geometry distinguishes them from normal headings and
    # avoids painting white boxes over the watermark.
    if region.region_kind == "line":
        return False

    contains_seal_marker = any(
        marker in normalized for marker in OFFICIAL_SEAL_MARKERS
    ) or (
        "REPUBLICA DE COSTA RICA" in normalized
        or "REPÚBLICA DE COSTA RICA" in normalized
    )
    if not contains_seal_marker:
        return False

    # A normal heading is a tight, one-line OCR box.  Seal/certification
    # artwork is typically returned as a taller composite box.  The previous
    # marker-only check incorrectly protected headings such as REPUBLICA DE
    # COSTA RICA and left them untranslated on every registry page.
    if not region.polygon or not region.page_height or region.page_height <= 0:
        return True
    _, top, _, bottom = polygon_bounds(region.polygon)
    relative_height = max(0.0, bottom - top) / region.page_height
    return (
        relative_height >= 0.018
        and len(normalized) <= 120
        and uppercase_ratio(normalized) >= 0.65
    )


def is_dense_structured_record(text: str) -> bool:
    """Detect tables/forms that must wait for cell-level translation.

    Positioned paragraph OCR merges several cells into one region. Translating
    such a merged record destroys its visual structure, so this first phase
    preserves it until the table extractor can provide individual cells.
    """

    normalized = normalize_translation_text(text).upper()
    label_count = sum(
        label in normalized
        for label in (
            "TOMO",
            "ASIENTO",
            "SECUENCIA",
            "FECHA",
            "FOLIO",
            "CITA",
            "IDENTIFICACIÓN",
            "IDENTIFICACION",
            "CÉDULA",
            "CEDULA",
            "NÚMERO",
            "NUMERO",
        )
    )
    digit_groups = len(re.findall(r"\d+[\d./-]*", normalized))
    dense_uppercase_form = (
        len(normalized) >= 180
        and uppercase_ratio(normalized) >= 0.65
        and (label_count >= 3 or digit_groups >= 7)
    )
    # Long legal prose often mentions an identity card, a law number, and a
    # date. That is narrative text, not a table, even though it contains the
    # same labels as a form. Keep it translatable unless the record is compact.
    compact_table_record = (
        len(normalized) <= 420 and label_count >= 2 and digit_groups >= 3
    )
    return dense_uppercase_form or compact_table_record


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


def is_compact_data_row(region: OCRRegion) -> bool:
    return is_compact_data_row_geometry(region.source_text, region.polygon)


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

    # Temporarily disabled: Google Document AI currently returns 0.0 for the
    # positioned paragraph regions, which would preserve every OCR result.
    # Keep this gate for restoration once the provider exposes paragraph-level
    # confidence values.
    # if region.confidence is not None and region.confidence < minimum_confidence:
    #     return "low_ocr_confidence"
    if is_non_latin_ocr_artifact(region.source_text):
        return "non_latin_ocr_artifact"
    if is_sparse_oversized_ocr_artifact(region):
        return "corrupted_ocr_text"
    if is_signature_initials(region):
        return "signature_or_initials"
    if (
        is_security_or_machine_text(region.source_text)
        and not translate_structured_form_labels(region.source_text)
    ):
        return "security_or_machine_text"
    if is_official_seal(region):
        return "official_seal"
    if is_issuer_branding(region):
        return "issuer_branding"
    # Structured paragraphs are split into OCR lines before reaching this
    # function.  A remaining paragraph is protected only as a last resort;
    # line regions are safe to translate independently without flattening a
    # table into one text block.
    if region.region_kind != "line" and is_dense_structured_record(region.source_text):
        return "structured_record_pending_cell_ocr"
    if region.region_kind != "line" and is_compact_data_row(region):
        return "structured_record_pending_cell_ocr"
    if low_text_quality(region.source_text):
        return "corrupted_ocr_text"
    return None


def preserve_structured_table_neighborhoods(regions: Sequence[OCRRegion]) -> None:
    """Preserve every OCR region in a detected table, not merely its seed row."""

    structured_by_page: dict[int, list[tuple[float, float]]] = {}
    for region in regions:
        if region.skip_reason != "structured_record_pending_cell_ocr":
            continue
        _, top, _, bottom = polygon_bounds(region.polygon)
        structured_by_page.setdefault(region.page_number, []).append((top, bottom))

    for page_number, spans in structured_by_page.items():
        merged_spans: list[list[float]] = []
        for top, bottom in sorted(spans):
            if merged_spans and top <= merged_spans[-1][1] + 140:
                merged_spans[-1][1] = max(merged_spans[-1][1], bottom)
            else:
                merged_spans.append([top, bottom])
        for region in regions:
            if (
                region.page_number != page_number
                or region.skip_reason
                or region.translated_text
            ):
                continue
            _, top, _, bottom = polygon_bounds(region.polygon)
            if any(top <= span_bottom + 35 and bottom >= span_top - 35 for span_top, span_bottom in merged_spans):
                region.skip_reason = "structured_table_neighborhood"


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


def translate_blocks(
    blocks: list[DocumentBlock],
    engine: TranslationEngine,
    batch_size: int,
    maximum_chunk_size: int,
) -> int:
    chunks = build_translation_chunks(blocks, engine, maximum_chunk_size)
    total_batches = (len(chunks) + batch_size - 1) // batch_size

    for batch_index, chunk_batch in enumerate(batched(chunks, batch_size), start=1):
        translations = engine.translate_batch(
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


def polygon_bounds(polygon: Sequence[Sequence[float]]) -> tuple[float, float, float, float]:
    xs = [point[0] for point in polygon]
    ys = [point[1] for point in polygon]
    return min(xs), min(ys), max(xs), max(ys)


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


def is_web_control_composite(text: str) -> bool:
    """Detect a merged issuance/status paragraph with embedded web controls."""

    normalized = normalize_translation_text(text).upper()
    return "EMITIDO" in normalized and any(
        control in normalized for control in ("IMPRIMIR", "REGRESAR", "COMPRAR")
    )


def layout_regions_for_page(page) -> list[tuple[object, str]]:
    """Prefer paragraphs, but split composite form/table regions into lines."""

    selected: list[tuple[object, str]] = []
    selected_keys: set[tuple[str, tuple[float, float, float, float]]] = set()
    page_text = normalize_translation_text(page.source_text).upper()
    is_civil_registry_form = (
        "CERTIFICA" in page_text
        and (
            "REGISTRO DE NACIMIENTOS" in page_text
            or "REGISTRO DENACIMIENTOS" in page_text
        )
    )
    is_identity_card_form = (
        ("CÉDULA DE IDENTIDAD" in page_text or "CEDULA DE IDENTIDAD" in page_text)
        and ("NÚMERO DE CÉDULA" in page_text or "NUMERO DE CEDULA" in page_text)
    )

    def append_region(region, region_kind: str) -> None:
        bounds = tuple(round(value, 2) for value in polygon_bounds(region.polygon))
        key = (normalize_translation_text(region.text), bounds)
        if key in selected_keys:
            return
        selected_keys.add(key)
        selected.append((region, region_kind))

    for paragraph in page.regions:
        _, top, _, bottom = polygon_bounds(paragraph.polygon)
        relative_top = top / page.height if page.height else 0
        relative_bottom = bottom / page.height if page.height else 0
        # Government certificates and identity cards place multiple values in
        # narrow rows. Paragraph OCR often merges adjacent rows, causing a
        # translated date, person name, and nationality to be painted into one
        # box. Use the provider's line geometry in the form body while keeping
        # narrative paragraphs intact for translation quality.
        multiline_form_body = "\n" in paragraph.text and (
            (
                is_civil_registry_form
                and relative_top >= 0.50
                and relative_bottom <= 0.80
            )
            or (
                is_identity_card_form
                and relative_top >= 0.45
                and relative_bottom <= 0.80
            )
        )
        should_split_into_lines = (
            is_web_control_composite(paragraph.text)
            or is_oversized_ocr_region(paragraph, page.width, page.height)
            or is_dense_structured_record(paragraph.text)
            or is_compact_data_row_geometry(paragraph.text, paragraph.polygon)
            or multiline_form_body
        )
        if should_split_into_lines:
            contained_lines = [
                line for line in page.lines if line_belongs_to_region(line, paragraph)
            ]
            if contained_lines:
                for line in contained_lines:
                    append_region(line, "line")
                continue
        append_region(paragraph, "paragraph")
    return selected


def extract_local_pdf_regions(
    input_path: Path,
) -> tuple[list[OCRRegion], str]:
    """OCR local pages as positioned paragraphs for layout-preserving export."""

    if not input_path.exists():
        raise FileNotFoundError(f"Input PDF does not exist: {input_path}")
    if input_path.suffix.lower() != ".pdf":
        raise ValueError("Local input must use the .pdf extension.")

    print("ocr_engine: shared document OCR provider", flush=True)
    try:
        ocr_result = extract_document_ocr(
            input_path.read_bytes(),
            source_language="es",
            rotations=(0,),
        )
    except ValueError as exc:
        raise TranslationError(str(exc)) from exc

    regions: list[OCRRegion] = []
    for page in ocr_result.pages:
        page_regions = layout_regions_for_page(page)
        normalized_page_text = normalize_translation_text(page.source_text).upper()
        is_civil_registry_form = (
            "CERTIFICA" in normalized_page_text
            and (
                "REGISTRO DE NACIMIENTOS" in normalized_page_text
                or "REGISTRO DENACIMIENTOS" in normalized_page_text
            )
        )
        for region, region_kind in page_regions:
            source_text = clean_translation_source_text(region.text)
            if source_text:
                polygon = [list(point) for point in region.polygon]
                # One certificate scan loses ``DÍA`` behind a vertical OCR
                # artifact and returns only ``EL``. Reconstruct the known form
                # label and extend its narrow box up to (but not into) the
                # adjacent date value.
                if (
                    is_civil_registry_form
                    and source_text.upper() == "EL"
                ):
                    left, top, right, _ = polygon_bounds(polygon)
                    relative_top = top / page.height if page.height else 0
                    if 0.60 <= relative_top <= 0.70:
                        source_text = "EL DÍA:"
                        expanded_right = min(page.width, max(right, left + 100))
                        polygon = [
                            [expanded_right if x >= right - 1 else x, y]
                            for x, y in polygon
                        ]
                regions.append(
                    OCRRegion(
                        page_number=page.page_number,
                        polygon=polygon,
                        source_text=source_text,
                        confidence=region.confidence,
                        page_width=page.width,
                        page_height=page.height,
                        render_scale=page.render_scale,
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

    apply_glossary_context(regions, minimum_ocr_confidence)
    for region in regions:
        region.skip_reason = region_skip_reason(region, minimum_ocr_confidence)
        fixed_translation = FIXED_REGION_TRANSLATIONS.get(region.source_text.upper())
        machine_value_translation = translate_machine_value(region.source_text)
        issued_ui_translation = translate_issued_ui_region(region.source_text)
        # Never paint a translation over logos, security artwork, signatures,
        # or unsegmented tables, even if a glossary contains matching words.
        if machine_value_translation:
            region.translated_text = (
                translate_structured_form_labels(machine_value_translation)
                or machine_value_translation
            )
            region.skip_reason = None
        elif issued_ui_translation and not region.skip_reason:
            region.translated_text = issued_ui_translation
        elif fixed_translation and not region.skip_reason:
            region.translated_text = fixed_translation
        elif region.source_text.startswith("#Acuerdo "):
            region.translated_text = region.source_text.replace(
                "#Acuerdo", "#Agreement", 1
            )
        elif region.skip_reason not in {
            "official_seal",
            "issuer_branding",
            "security_or_machine_text",
            "signature_or_initials",
        }:
            structured_translation = translate_structured_form_labels(
                region.source_text
            )
            if structured_translation:
                region.translated_text = postprocess_translation(
                    structured_translation
                )
                if region.skip_reason in {
                    "structured_record_pending_cell_ocr",
                    "structured_table_neighborhood",
                }:
                    region.skip_reason = None

    preserve_structured_table_neighborhoods(regions)
    for region in regions:
        if not region.glossary_translation:
            continue
        # A glossary can safely translate known labels inside a table, but it
        # never overwrites logos, seals, security artwork, or signatures.
        if region.skip_reason in {
            "official_seal",
            "issuer_branding",
            "security_or_machine_text",
            "signature_or_initials",
        }:
            continue
        if (
            region.skip_reason
            in {
                "structured_record_pending_cell_ocr",
                "structured_table_neighborhood",
            }
            and not region.glossary_allows_structured_region
        ):
            continue
        region.translated_text = region.glossary_translation
        region.skip_reason = None

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


PROTECTED_RENDER_REASONS = {
    "official_seal",
    "issuer_branding",
    "security_or_machine_text",
    "signature_or_initials",
    "structured_record_pending_cell_ocr",
    "structured_table_neighborhood",
}


def calibrated_pdf_rect(
    region: OCRRegion,
    source_page,
) -> pymupdf.Rect | None:
    """Map provider-native OCR coordinates to the actual PDF page rectangle."""

    if not region.page_width or not region.page_height:
        return None
    if region.page_width <= 0 or region.page_height <= 0 or not region.polygon:
        return None

    x_scale = source_page.rect.width / region.page_width
    y_scale = source_page.rect.height / region.page_height
    xs = [source_page.rect.x0 + point[0] * x_scale for point in region.polygon]
    ys = [source_page.rect.y0 + point[1] * y_scale for point in region.polygon]
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
    return pymupdf.Rect(rect.x0 - 0.5, rect.y0 - 0.5, rect.x1 + 0.5, rect.y1 + 0.5)


def overlap_ratio(first: pymupdf.Rect, second: pymupdf.Rect) -> float:
    intersection = first & second
    if intersection.is_empty or intersection.width <= 0 or intersection.height <= 0:
        return 0.0
    smallest_area = min(first.width * first.height, second.width * second.height)
    if smallest_area <= 0:
        return 0.0
    return (intersection.width * intersection.height) / smallest_area


def source_line_count(region: OCRRegion) -> int:
    return max(1, len([line for line in region.source_text.splitlines() if line.strip()]))


def fit_translation_shape(page, region: OCRRegion, rect: pymupdf.Rect):
    """Return a fitted text shape, preserving the source when it cannot fit."""

    line_count = source_line_count(region)
    source_line_height = rect.height / max(1, line_count)
    maximum_font_size = 9 if region.region_kind == "line" else 10
    is_tiny_ui_control = normalize_translation_text(region.source_text).upper() in {
        "IMPRIMIR",
        "REGRESAR",
        "COMPRAR",
    }
    # OCR boxes are frequently only three to five PDF points high.  A hard
    # five-point minimum made PyMuPDF reject otherwise valid short labels and
    # silently left their Spanish scan visible.  Half-point steps retain the
    # largest readable size that actually fits the source geometry.
    minimum_font_size = 3.0 if region.region_kind == "line" else 3.5
    if is_tiny_ui_control:
        minimum_font_size = 3.0
    starting_font_size = min(
        maximum_font_size,
        max(minimum_font_size, source_line_height * 0.60),
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
            color=(0, 0, 0),
        )
        if remaining >= 0:
            return shape
    return None


def build_page_background(page, source_page) -> None:
    """Render the source scan at the PDF's native page size."""

    background = source_page.get_pixmap(alpha=False)
    page.insert_image(page.rect, pixmap=background)


def export_coordinate_debug_pdf(
    input_path: Path,
    regions: Sequence[OCRRegion],
    output_path: Path,
) -> None:
    """Write a visual calibration artifact before any text is translated."""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists():
        output_path.unlink()
    source_document = pymupdf.open(input_path)
    document = pymupdf.open()
    regions_by_page: dict[int, list[OCRRegion]] = {}
    for region in regions:
        regions_by_page.setdefault(region.page_number, []).append(region)

    for page_index, source_page in enumerate(source_document):
        page = document.new_page(
            width=source_page.rect.width,
            height=source_page.rect.height,
        )
        build_page_background(page, source_page)
        for index, region in enumerate(regions_by_page.get(page_index + 1, []), start=1):
            rect = calibrated_pdf_rect(region, source_page)
            if rect is None:
                continue
            color = (0, 0.45, 0.95) if region.region_kind == "paragraph" else (0, 0.65, 0.15)
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
    source_document.close()


def export_positioned_translation_pdf(
    input_path: Path,
    regions: Sequence[OCRRegion],
    output_path: Path,
) -> RenderQualityReport:
    """Overlay fitted English translations on calibrated source text regions."""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists():
        output_path.unlink()
    source_document = pymupdf.open(input_path)
    document = pymupdf.open()
    regions_by_page: dict[int, list[OCRRegion]] = {}
    for region in regions:
        regions_by_page.setdefault(region.page_number, []).append(region)
    report = RenderQualityReport()

    for page_index, source_page in enumerate(source_document):
        page_number = page_index + 1
        page_regions = regions_by_page.get(page_number, [])
        page = document.new_page(
            width=source_page.rect.width,
            height=source_page.rect.height,
        )
        build_page_background(page, source_page)
        protected_rects = []
        for region in page_regions:
            if region.skip_reason not in PROTECTED_RENDER_REASONS:
                continue
            rect = calibrated_pdf_rect(region, source_page)
            if rect is not None:
                protected_rects.append(rect)
        report.protected_regions += len(protected_rects)

        for region in page_regions:
            if not region.translated_text:
                continue
            rect = calibrated_pdf_rect(region, source_page)
            if rect is None:
                region.skip_reason = "invalid_render_coordinates"
                region.translated_text = ""
                report.preserved_invalid_coordinates += 1
                continue
            if region.region_kind == "paragraph" and (
                rect.width * rect.height > page.rect.width * page.rect.height * 0.30
            ):
                region.skip_reason = "oversized_render_region"
                region.translated_text = ""
                report.preserved_oversized_region += 1
                continue
            if any(overlap_ratio(rect, protected_rect) >= 0.22 for protected_rect in protected_rects):
                region.skip_reason = "protected_zone_overlap"
                region.translated_text = ""
                report.preserved_protected_overlap += 1
                continue
            fitted_shape = fit_translation_shape(page, region, rect)
            if fitted_shape is None:
                region.skip_reason = "translation_does_not_fit"
                region.translated_text = ""
                report.preserved_does_not_fit += 1
                continue
            page.draw_rect(rect, color=None, fill=(1, 1, 1), overlay=True)
            fitted_shape.commit(overlay=True)
            report.translated_regions += 1

    document.save(output_path, garbage=4, deflate=True)
    document.close()
    source_document.close()
    return report


def run_local_pdf_translation(args: argparse.Namespace) -> None:
    if args.output_pdf.suffix.lower() != ".pdf":
        raise ValueError("Local output must use the .pdf extension.")

    regions, provider = extract_local_pdf_regions(args.input_pdf)
    source_pages = len({region.page_number for region in regions})
    total_characters = sum(len(region.source_text) for region in regions)

    print("\n=== local extraction summary ===")
    print(f"provider: {provider}")
    print(f"source_pages: {source_pages}")
    print(f"ocr_regions: {len(regions)}")
    print(f"output_chars: {total_characters}")
    confidence_values = [region.confidence for region in regions if region.confidence is not None]
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
    glossary_matches = Counter(
        region.glossary_match for region in regions if region.glossary_match
    )

    print("\n=== local translation summary ===")
    print(f"source_pages: {source_pages}")
    print(f"translation_chunks: {chunk_count}")
    print(f"translated_regions: {sum(bool(region.translated_text) for region in regions)}")
    print(f"preserved_source_regions: {len(regions) - sum(bool(region.translated_text) for region in regions)}")
    print(f"glossary_context_matches: {sum(glossary_matches.values())}")
    for match, count in sorted(glossary_matches.items()):
        print(f"glossary_{match}: {count}")
    for reason, count in sorted(skipped_regions.items()):
        print(f"preserved_{reason}: {count}")
    preserved_samples = sorted(
        [
        region
        for region in regions
        if region.skip_reason
        and region.skip_reason not in {"signature_or_initials"}
        ],
        key=lambda region: (
            region.skip_reason not in {
                "invalid_render_coordinates",
                "protected_zone_overlap",
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
    print(f"protected_render_regions: {render_report.protected_regions}")
    print(
        "preserved_invalid_render_coordinates: "
        f"{render_report.preserved_invalid_coordinates}"
    )
    print(
        "preserved_protected_zone_overlap: "
        f"{render_report.preserved_protected_overlap}"
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
    if not 1 <= args.google_max_input_characters <= GOOGLE_TRANSLATE_REQUEST_CHARACTER_LIMIT:
        raise ValueError(
            "google_max_input_characters must be between 1 and "
            f"{GOOGLE_TRANSLATE_REQUEST_CHARACTER_LIMIT}."
        )
    if not 0 <= args.ocr_min_confidence <= 1:
        raise ValueError("ocr_min_confidence must be between 0 and 1.")
    run_local_pdf_translation(args)


if __name__ == "__main__":
    main()
