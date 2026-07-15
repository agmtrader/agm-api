from __future__ import annotations

import argparse
import os
import re
import unicodedata
from collections import Counter
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import Iterable, Protocol, Sequence

import pymupdf
import torch
from dotenv import load_dotenv
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
TRANSLATION_SOURCE_TEXT_REPLACEMENTS = {
    "YNOTARIOS": "Y NOTARIOS",
    "ACUERDO DE CONCILIACION": "ACUERDO DE CONCILIACIÓN",
    "aqosto": "agosto",
    "deL": "del",
    "SQLIS": "SOLÍS",
    "ROCIQ": "ROCÍO",
    "Ias": "las",
}
FIXED_REGION_TRANSLATIONS = {
    "E&V": "E&V",
    "ABOGADOS Y NOTARIOS": "LAWYERS AND NOTARIES",
    "ACUERDO DE CONCILIACIÓN:": "CONCILIATION AGREEMENT:",
    "CONCILIADOR": "CONCILIATOR",
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
    page_height: float | None = None
    render_scale: float | None = None
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
        "NO RELEASES",
        (
            "NO POSEE LEVANIAMIENTO(S)",
            "NO POSCE LEVANIAMIENTO(S)",
            "NO POSCE LEVANTAMIENTO(S)",
        ),
    ),
    GlossaryEntry("TOMO", "VOLUME"),
    GlossaryEntry("FOLIO", "FOLIO"),
    GlossaryEntry("ASIENTO", "ENTRY"),
    GlossaryEntry("CITA", "REFERENCE"),
    GlossaryEntry("FECHA", "DATE"),
    GlossaryEntry("GRAVAMEN(ES)", "ENCUMBRANCE(S)"),
    GlossaryEntry("DENUNCIA DE TRÁNSITO", "TRAFFIC REPORT", ("DENUNCIA DE TRANSITO",)),
)


class TranslationError(RuntimeError):
    pass


class TranslationEngine(Protocol):
    @property
    def tokenizer(self) -> PreTrainedTokenizerBase:
        ...

    def translate_batch(self, texts: Sequence[str]) -> list[str]:
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

    @property
    def tokenizer(self) -> PreTrainedTokenizerBase:
        return self._tokenizer

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


def preview_text(value: str) -> str:
    normalized = (value or "").strip()
    if not normalized:
        return "<empty>"
    return normalized[:PREVIEW_LENGTH]


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "OCR a scanned Spanish PDF, translate it to English with MarianMT, "
            "and preserve the original page layout in the output PDF."
        )
    )
    parser.add_argument("input_pdf", type=Path, help="Source Spanish PDF")
    parser.add_argument("output_pdf", type=Path, help="Translated English PDF")
    parser.add_argument("--model", default=DEFAULT_MODEL)
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


def clean_translation_source_text(text: str) -> str:
    """Repair recurring scan artifacts before sending text to translation."""

    cleaned = str(text or "")
    cleaned = re.sub(r"_+", " ", cleaned)
    for incorrect, replacement in TRANSLATION_SOURCE_TEXT_REPLACEMENTS.items():
        cleaned = cleaned.replace(incorrect, replacement)
    return normalize_translation_text(cleaned)


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


def is_signature_initials(text: str) -> bool:
    """Avoid covering handwritten signature initials with machine translation."""

    compact = re.sub(r"[^A-Za-z]", "", str(text or ""))
    return 1 <= len(compact) <= 5


def uppercase_ratio(text: str) -> float:
    letters = [character for character in str(text or "") if character.isalpha()]
    if not letters:
        return 0.0
    return sum(character.isupper() for character in letters) / len(letters)


def is_security_or_machine_text(text: str) -> bool:
    """Keep URLs, machine codes, and QR/barcode-adjacent OCR untouched."""

    normalized = normalize_translation_text(text).lower()
    compact = re.sub(r"\s+", "", normalized)
    return bool(
        re.search(r"(?:https?://|www\.|@|\bqr\b|\bvin\b)", normalized)
        or (
            len(compact) >= 12
            and any(character.isdigit() for character in compact)
            and re.fullmatch(r"[a-z0-9/_\-.:]+", compact)
        )
    )


def is_issuer_branding(region: OCRRegion) -> bool:
    """Preserve government/issuer branding and tiny margin labels as artwork."""

    normalized = normalize_translation_text(region.source_text).upper()
    issuer_markers = (
        "TRIBUNAL SUPREMO DE ELECCIONES",
        "REGISTRO CIVIL",
        "REPUBLICA DE COSTA RICA",
        "REPÚBLICA DE COSTA RICA",
    )
    if any(marker in normalized for marker in issuer_markers):
        return True

    if region.page_height is None or region.page_height <= 0:
        return False
    top = min(point[1] for point in region.polygon)
    bottom = max(point[1] for point in region.polygon)
    is_margin = top <= region.page_height * 0.13 or bottom >= region.page_height * 0.93
    return is_margin and len(normalized) <= 90 and uppercase_ratio(normalized) >= 0.70


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
    compact_table_record = label_count >= 2 and digit_groups >= 3
    return dense_uppercase_form or compact_table_record


def is_compact_data_row(region: OCRRegion) -> bool:
    """Keep a short, wide OCR row that mixes labels with identifiers intact."""

    left, top, right, bottom = polygon_bounds(region.polygon)
    width = max(1, right - left)
    height = max(1, bottom - top)
    has_identifier = bool(re.search(r"\d{4,}|\d{1,2}[-/]\w+[-/]\d{2,4}", region.source_text))
    return (
        has_identifier
        and width / height >= 8
        and len(re.findall(r"[A-Za-zÀ-ÿ]+", region.source_text)) >= 6
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

    if region.confidence is not None and region.confidence < minimum_confidence:
        return "low_ocr_confidence"
    if is_signature_initials(region.source_text):
        return "signature_or_initials"
    if is_security_or_machine_text(region.source_text):
        return "security_or_machine_text"
    if is_issuer_branding(region):
        return "issuer_branding"
    if is_dense_structured_record(region.source_text):
        return "structured_record_pending_cell_ocr"
    if is_compact_data_row(region):
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


def build_translation_chunks(
    blocks: Sequence[DocumentBlock],
    tokenizer: PreTrainedTokenizerBase,
    max_tokens: int,
) -> list[TranslationChunk]:
    chunks = []
    for block_index, block in enumerate(blocks):
        block_chunks = create_token_safe_chunks(
            block.source_text,
            tokenizer,
            max_tokens,
        )
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
    max_input_tokens: int,
) -> int:
    chunks = build_translation_chunks(blocks, engine.tokenizer, max_input_tokens)
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
            translated_text = translated_text.strip()
            if not translated_text:
                raise TranslationError(
                    f"The model returned an empty translation for block "
                    f"{chunk.block_index}."
                )
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
        block.translated_text = " ".join(
            chunk.translated_text for chunk in block_chunks
        ).strip()
    return len(chunks)


def polygon_bounds(polygon: Sequence[Sequence[float]]) -> tuple[float, float, float, float]:
    xs = [point[0] for point in polygon]
    ys = [point[1] for point in polygon]
    return min(xs), min(ys), max(xs), max(ys)


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
        for region in page.regions:
            source_text = clean_translation_source_text(region.text)
            if source_text:
                regions.append(
                    OCRRegion(
                        page_number=page.page_number,
                        polygon=region.polygon,
                        source_text=source_text,
                        confidence=region.confidence,
                        page_height=page.height,
                        render_scale=page.render_scale,
                    )
                )
        print(
            f"ocr_positioned_page_result: page={page.page_number} "
            f"regions={len(page.regions)}",
            flush=True,
        )

    if not regions:
        raise TranslationError("PDF OCR completed but produced no positioned text.")
    return regions, ocr_result.provider


def translate_regions(
    regions: list[OCRRegion],
    engine: TranslationEngine,
    batch_size: int,
    max_input_tokens: int,
    minimum_ocr_confidence: float,
) -> int:
    """Translate OCR regions while preserving their page-coordinate mapping."""

    apply_glossary_context(regions, minimum_ocr_confidence)
    for region in regions:
        fixed_translation = FIXED_REGION_TRANSLATIONS.get(region.source_text.upper())
        if fixed_translation:
            region.translated_text = fixed_translation
        elif region.source_text.startswith("#Acuerdo "):
            region.translated_text = region.source_text.replace(
                "#Acuerdo", "#Agreement", 1
            )
        else:
            region.skip_reason = region_skip_reason(region, minimum_ocr_confidence)

    preserve_structured_table_neighborhoods(regions)
    for region in regions:
        if not region.glossary_translation:
            continue
        # A glossary can recover a known label from weak OCR, but it never
        # overrides security artwork, signatures, or an unsegmented table.
        if region.skip_reason in {
            "issuer_branding",
            "security_or_machine_text",
            "signature_or_initials",
            "structured_record_pending_cell_ocr",
        }:
            continue
        if (
            region.skip_reason == "structured_table_neighborhood"
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
        max_input_tokens=max_input_tokens,
    )
    for region, block in zip(translatable_regions, blocks, strict=True):
        region.translated_text = block.translated_text
    return chunk_count


def export_positioned_translation_pdf(
    input_path: Path,
    regions: Sequence[OCRRegion],
    output_path: Path,
) -> None:
    """Overlay fitted English translations on the original scanned pages."""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists():
        output_path.unlink()
    source_document = pymupdf.open(input_path)
    document = pymupdf.open()
    regions_by_page: dict[int, list[OCRRegion]] = {}
    for region in regions:
        regions_by_page.setdefault(region.page_number, []).append(region)

    for page_index, source_page in enumerate(source_document):
        page_number = page_index + 1
        page_regions = regions_by_page.get(page_number, [])
        page_render_scale = next(
            (
                region.render_scale
                for region in page_regions
                if region.render_scale is not None
            ),
            1.0,
        )
        page = document.new_page(
            width=source_page.rect.width,
            height=source_page.rect.height,
        )
        background = source_page.get_pixmap(
            matrix=pymupdf.Matrix(page_render_scale, page_render_scale),
            alpha=False,
        )
        page.insert_image(page.rect, pixmap=background)

        for region in page_regions:
            if not region.translated_text:
                continue
            region_scale = region.render_scale or page_render_scale
            xs = [point[0] / region_scale for point in region.polygon]
            ys = [point[1] / region_scale for point in region.polygon]
            rect = pymupdf.Rect(
                max(0, min(xs) - 1),
                max(0, min(ys) - 1),
                min(page.rect.width, max(xs) + 1),
                min(page.rect.height, max(ys) + 1),
            )
            if rect.width < 8 or rect.height < 5:
                continue

            text_length = len(region.translated_text)
            if text_length > 180:
                starting_font_size = 12
            elif text_length > 60:
                starting_font_size = 12
            else:
                starting_font_size = min(18, max(6, int(rect.height * 0.72)))
            fitted_shape = None
            for font_size in range(starting_font_size, 3, -1):
                shape = page.new_shape()
                remaining = shape.insert_textbox(
                    rect,
                    region.translated_text,
                    fontname="helv",
                    fontsize=font_size,
                    lineheight=1.05,
                    color=(0, 0, 0),
                )
                if remaining >= 0:
                    fitted_shape = shape
                    break
            if fitted_shape is None:
                # Do not erase readable source text merely because English does
                # not fit in its original scan bounding box.
                region.skip_reason = "translation_does_not_fit"
                region.translated_text = ""
                continue
            page.draw_rect(rect, color=None, fill=(1, 1, 1), overlay=True)
            fitted_shape.commit(overlay=True)

    document.save(output_path, garbage=4, deflate=True)
    document.close()
    source_document.close()


def run_local_pdf_translation(args: argparse.Namespace) -> None:
    if args.output_pdf.suffix.lower() != ".pdf":
        raise ValueError("Local output must use the .pdf extension.")

    device = select_device(args.device)
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
            "chars": len(region.source_text),
            "preview": preview_text(region.source_text),
        })

    print(f"translation_device: {device}")
    print(f"loading_translation_model: {args.model}")
    engine = MarianTransformersEngine(
        model_name=args.model,
        device=device,
        max_input_tokens=args.max_input_tokens,
    )
    chunk_count = translate_regions(
        regions,
        engine=engine,
        batch_size=args.batch_size,
        max_input_tokens=args.max_input_tokens,
        minimum_ocr_confidence=args.ocr_min_confidence,
    )
    export_positioned_translation_pdf(args.input_pdf, regions, args.output_pdf)
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
    print(f"translated_pdf: {args.output_pdf}")



def main() -> None:
    args = parse_arguments()
    if args.batch_size < 1:
        raise ValueError("batch_size must be at least 1.")
    if args.max_input_tokens < 32:
        raise ValueError("max_input_tokens must be at least 32.")
    if not 0 <= args.ocr_min_confidence <= 1:
        raise ValueError("ocr_min_confidence must be between 0 and 1.")
    run_local_pdf_translation(args)


if __name__ == "__main__":
    main()
