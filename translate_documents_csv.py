from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import json
import re
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from dotenv import load_dotenv


PROJECT_DIRECTORY = Path(__file__).resolve().parent
load_dotenv(PROJECT_DIRECTORY / ".env", override=False)

DEFAULT_MODEL = "translategemma:4b"
DEFAULT_CONTEXT_PATH = PROJECT_DIRECTORY / "context.md"
DEFAULT_MAX_SOURCE_CHARACTERS = 2_200
DEFAULT_OLLAMA_TIMEOUT_SECONDS = 600
GOOGLE_DOCUMENT_AI_PROVIDER = "google_document_ai_enterprise_ocr"
TEXT_EXTRACTION_PROCESS_TYPE = "text_extraction"

CSV_FIELDS = (
    "document_id",
    "document_hash",
    "source_file",
    "ocr_provider",
    "page_count",
    "source_characters",
    "translated_characters",
    "status",
    "translated_text",
    "error",
)

SPANISH_MONTHS = (
    "enero",
    "febrero",
    "marzo",
    "abril",
    "mayo",
    "junio",
    "julio",
    "agosto",
    "septiembre",
    "setiembre",
    "octubre",
    "noviembre",
    "diciembre",
)
DATE_PATTERN = re.compile(
    rf"\b(?:\d{{1,2}}\s+DE\s+)?(?:{'|'.join(SPANISH_MONTHS)})"
    rf"(?:\s+DE(?:L)?\s+|\s+)\d{{4}}\b",
    re.IGNORECASE,
)
MRZ_LINE_PATTERN = re.compile(r"(?m)^[A-Z0-9<]{30,}$")
PAGE_COUNTER_PATTERN = re.compile(r"Page\s+\d+\s+of\s+\d+", re.IGNORECASE)
DIGITAL_SIGNATURE_BLOCK_PATTERN = re.compile(
    r"(?im)^[^\n]*(?:digitally signed by|firmado digitalmente por)[^\n]*"
    r"(?:\n[^\n]*){0,8}$"
)
SIGNATURE_LINE_PATTERN = re.compile(
    r"(?im)^.*(?:digitally signed by|firmado digitalmente por|\(firma\)).*$"
)
TITLED_NAME_PATTERN = re.compile(
    r"\b(?:Lcda|Lcdo|Sr|Sra|Dr|Dra)\.[ \t]+"
    r"[A-ZÁÉÍÓÚÜÑ][a-záéíóúüñ]+"
    r"(?:[ \t]+[A-ZÁÉÍÓÚÜÑ][a-záéíóúüñ]+){1,4}\b"
)
LEGAL_ENTITY_LINE_PATTERN = re.compile(
    r"(?im)^[^\n]{1,120}\b(?:S\.?\s*R\.?\s*L\.?|S\.?\s*A\.?|"
    r"L\.?L\.?C\.?|I\.?N\.?C\.?)$"
)
MACHINE_VALUE_PATTERN = re.compile(
    r"(?<![\w])(?:[₡$€£]\s*)?[-+]?\d[\d.,'/:+-]*(?:\s*%)?(?![\w])"
)
CODE_PATTERN = re.compile(
    r"\b(?=[A-Z0-9._/-]{4,}\b)(?=[A-Z0-9._/-]*[A-Z])"
    r"(?=[A-Z0-9._/-]*\d)"
    r"[A-Z0-9]+(?:[._/-][A-Z0-9]+)+\b"
)
OUTPUT_MARKER_PATTERN = re.compile(
    r"(?im)^\s*---\s*(?:DOCUMENT|CONTEXT)\s+(?:START|END)\s*---\s*$"
)


class DocumentTranslationError(RuntimeError):
    pass


@dataclass(slots=True)
class SourceDocument:
    document_id: str
    document_hash: str
    source_file: str
    ocr_provider: str
    page_count: int
    source_text: str


@dataclass(slots=True)
class ProtectedText:
    text: str
    values_by_placeholder: dict[str, str]


@dataclass(slots=True)
class TranslationUnit:
    text: str
    translate: bool


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Read OCR JSON files or database documents, translate Spanish OCR "
            "text with local TranslateGemma through Ollama, and write a CSV. "
            "Database access is read-only."
        )
    )
    parser.add_argument(
        "--ocr-json",
        action="append",
        type=Path,
        default=[],
        help="Google OCR JSON fixture. Repeat for multiple documents.",
    )
    parser.add_argument(
        "--document-id",
        action="append",
        default=[],
        help="Document UUID to read from the database. Repeat as needed.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Destination CSV path.",
    )
    parser.add_argument(
        "--context",
        type=Path,
        default=DEFAULT_CONTEXT_PATH,
        help="Reference glossary/context file (default: %(default)s).",
    )
    parser.add_argument(
        "--model",
        default=DEFAULT_MODEL,
        help="Ollama model tag (default: %(default)s).",
    )
    parser.add_argument(
        "--ocr-provider",
        default="google",
        help="OCR provider for uncached database documents (default: %(default)s).",
    )
    parser.add_argument(
        "--no-reuse-existing-ocr",
        action="store_true",
        help="Run OCR even when completed Google OCR text exists in the database.",
    )
    parser.add_argument(
        "--max-source-characters",
        type=int,
        default=DEFAULT_MAX_SOURCE_CHARACTERS,
        help=(
            "Maximum source characters in each model call. The context and "
            "instructions are added separately (default: %(default)s)."
        ),
    )
    parser.add_argument(
        "--ollama-timeout-seconds",
        type=int,
        default=DEFAULT_OLLAMA_TIMEOUT_SECONDS,
        help="Timeout for each Ollama call (default: %(default)s).",
    )
    return parser.parse_args()


def normalize_document_text(value: str) -> str:
    normalized = str(value or "").replace("\r\n", "\n").replace("\r", "\n")
    lines = [line.rstrip() for line in normalized.splitlines()]
    return "\n".join(lines).strip()


def sha256_hex(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def load_ocr_json_document(path: Path) -> SourceDocument:
    try:
        raw_payload = path.read_bytes()
        payload = json.loads(raw_payload)
    except OSError as exc:
        raise DocumentTranslationError(f"Unable to read OCR JSON {path}: {exc}") from exc
    except (TypeError, ValueError) as exc:
        raise DocumentTranslationError(f"Invalid OCR JSON {path}: {exc}") from exc

    pages = payload.get("pages") if isinstance(payload, dict) else None
    if not isinstance(pages, list) or not pages:
        raise DocumentTranslationError(f"OCR JSON has no pages: {path}")

    page_texts = []
    for page in pages:
        if not isinstance(page, dict):
            continue
        page_text = normalize_document_text(
            page.get("source_text") or page.get("text") or ""
        )
        if page_text:
            page_texts.append(page_text)
    source_text = "\n\n".join(page_texts).strip()
    if not source_text:
        raise DocumentTranslationError(f"OCR JSON contains no source text: {path}")

    document_id = str(payload.get("document_id") or path.stem).strip()
    document_hash = str(payload.get("source_hash") or "").strip() or sha256_hex(
        raw_payload
    )
    return SourceDocument(
        document_id=document_id,
        document_hash=document_hash,
        source_file=path.name,
        ocr_provider=str(payload.get("provider") or "ocr_json").strip(),
        page_count=len(page_texts),
        source_text=source_text,
    )


def decode_database_document(document_row: dict) -> bytes:
    encoded_data = str(document_row.get("data") or "").strip()
    if not encoded_data:
        raise DocumentTranslationError("Database document data is empty.")
    try:
        return base64.b64decode(encoded_data, validate=True)
    except (ValueError, TypeError) as exc:
        raise DocumentTranslationError("Database document data is invalid base64.") from exc


def completed_google_ocr_text(db, document_id: str) -> tuple[str, str] | None:
    rows = db.read(
        table="document_processing",
        query={
            "document_id": document_id,
            "process_type": TEXT_EXTRACTION_PROCESS_TYPE,
        },
    ) or []
    for row in rows:
        provider = str(row.get("provider") or "").strip()
        output_text = normalize_document_text(row.get("output_text") or "")
        if (
            str(row.get("status") or "").strip().lower() == "completed"
            and provider == GOOGLE_DOCUMENT_AI_PROVIDER
            and output_text
        ):
            return output_text, provider
    return None


def load_database_document(
    document_id: str,
    *,
    ocr_provider: str,
    reuse_existing_ocr: bool,
) -> SourceDocument:
    # Importing the connector initializes Secret Manager and the database, so
    # keep it out of local fixture-only runs.
    from src.utils.connectors.supabase import db

    rows = db.read(table="document", query={"id": document_id}) or []
    if not rows:
        raise DocumentTranslationError(f"Database document not found: {document_id}")
    document_row = rows[0]
    file_bytes = decode_database_document(document_row)
    document_hash = str(document_row.get("sha1_checksum") or "").strip()
    if not document_hash:
        document_hash = sha256_hex(file_bytes)

    cached_ocr = (
        completed_google_ocr_text(db, document_id) if reuse_existing_ocr else None
    )
    if cached_ocr is not None:
        source_text, provider = cached_ocr
        page_count = max(1, source_text.count("\f") + 1)
    else:
        # This is the same provider-neutral Google OCR boundary imported and
        # used by Translation.py. Calling it directly avoids the separate
        # process_document_text_extraction function, which writes to the DB.
        from Translation import extract_document_ocr

        mime_type = str(document_row.get("mime_type") or "").strip()
        ocr_result = extract_document_ocr(
            file_bytes,
            source_language="es",
            mime_type=mime_type,
            provider=ocr_provider,
            use_cache=True,
        )
        source_text = normalize_document_text(ocr_result.text)
        provider = ocr_result.provider
        page_count = len(ocr_result.pages)

    if not source_text:
        raise DocumentTranslationError(
            f"OCR produced no source text for database document {document_id}."
        )
    return SourceDocument(
        document_id=str(document_row.get("id") or document_id),
        document_hash=document_hash,
        source_file=str(document_row.get("file_name") or document_id),
        ocr_provider=provider,
        page_count=page_count,
        source_text=source_text,
    )


def placeholder_label(index: int) -> str:
    # Alphabetic labels prevent the numeric protection regex from matching a
    # placeholder during later protection passes.
    value = index + 1
    letters = []
    while value:
        value, remainder = divmod(value - 1, 26)
        letters.append(chr(ord("A") + remainder))
    return "".join(reversed(letters))


def protect_nontranslatable_values(source_text: str) -> ProtectedText:
    protected_text = source_text
    values_by_placeholder: dict[str, str] = {}

    def replacement(match: re.Match[str]) -> str:
        placeholder = f"__PRESERVE_{placeholder_label(len(values_by_placeholder))}__"
        values_by_placeholder[placeholder] = match.group(0)
        return placeholder

    # Protect broad line-level entities first, then smaller values. Later
    # patterns cannot match text that has already become a placeholder.
    for pattern in (
        MRZ_LINE_PATTERN,
        DIGITAL_SIGNATURE_BLOCK_PATTERN,
        SIGNATURE_LINE_PATTERN,
        LEGAL_ENTITY_LINE_PATTERN,
        TITLED_NAME_PATTERN,
        DATE_PATTERN,
        CODE_PATTERN,
        MACHINE_VALUE_PATTERN,
    ):
        protected_text = pattern.sub(replacement, protected_text)

    return ProtectedText(
        text=protected_text,
        values_by_placeholder=values_by_placeholder,
    )


def split_long_line(line: str, maximum_characters: int) -> list[str]:
    if len(line) <= maximum_characters:
        return [line]
    pieces = []
    remaining = line
    while len(remaining) > maximum_characters:
        split_at = remaining.rfind(" ", 0, maximum_characters + 1)
        if split_at <= 0:
            split_at = maximum_characters
        pieces.append(remaining[:split_at].rstrip())
        remaining = remaining[split_at:].lstrip()
    if remaining:
        pieces.append(remaining)
    return pieces


def split_source_text(source_text: str, maximum_characters: int) -> list[str]:
    chunks: list[str] = []
    current_lines: list[str] = []
    current_length = 0
    for original_line in source_text.splitlines():
        for line in split_long_line(original_line, maximum_characters):
            added_length = len(line) + (1 if current_lines else 0)
            if current_lines and current_length + added_length > maximum_characters:
                chunks.append("\n".join(current_lines).strip())
                current_lines = []
                current_length = 0
            current_lines.append(line)
            current_length += len(line) + (1 if len(current_lines) > 1 else 0)
    if current_lines:
        chunks.append("\n".join(current_lines).strip())
    return [chunk for chunk in chunks if chunk]


def digital_signature_line_indexes(source_text: str) -> set[int]:
    protected_indexes: set[int] = set()
    line_spans = []
    offset = 0
    for index, line in enumerate(source_text.splitlines(keepends=True)):
        line_spans.append((index, offset, offset + len(line)))
        offset += len(line)
    for match in DIGITAL_SIGNATURE_BLOCK_PATTERN.finditer(source_text):
        for index, start, end in line_spans:
            if start < match.end() and end > match.start():
                protected_indexes.add(index)
    return protected_indexes


def line_is_passthrough(line: str) -> bool:
    stripped = line.strip()
    if not stripped:
        return True
    if not re.search(r"[A-Za-zÀ-ÿ]", stripped):
        return True
    return any(
        pattern.fullmatch(stripped)
        for pattern in (
            MRZ_LINE_PATTERN,
            PAGE_COUNTER_PATTERN,
            SIGNATURE_LINE_PATTERN,
            LEGAL_ENTITY_LINE_PATTERN,
            TITLED_NAME_PATTERN,
            DATE_PATTERN,
            CODE_PATTERN,
        )
    )


def parse_exact_context_translations(context: str) -> dict[str, str]:
    translations: dict[str, str] = {}
    for line in context.splitlines():
        if "=" not in line:
            continue
        source, translated = (part.strip() for part in line.split("=", 1))
        if source and translated:
            translations[source.casefold()] = translated
    return translations


def build_translation_units(
    source_text: str,
    exact_translations: dict[str, str] | None = None,
) -> list[TranslationUnit]:
    signature_indexes = digital_signature_line_indexes(source_text)
    exact_translations = exact_translations or {}
    units: list[TranslationUnit] = []
    pending_translation_lines: list[str] = []
    pending_passthrough_lines: list[str] = []

    def flush_translation() -> None:
        if pending_translation_lines:
            units.append(
                TranslationUnit(
                    text="\n".join(pending_translation_lines),
                    translate=True,
                )
            )
            pending_translation_lines.clear()

    def flush_passthrough() -> None:
        if pending_passthrough_lines:
            units.append(
                TranslationUnit(
                    text="\n".join(pending_passthrough_lines),
                    translate=False,
                )
            )
            pending_passthrough_lines.clear()

    for line_index, line in enumerate(source_text.splitlines()):
        exact_translation = exact_translations.get(line.strip().casefold())
        if exact_translation is not None:
            flush_translation()
            flush_passthrough()
            units.append(TranslationUnit(text=exact_translation, translate=False))
            continue
        passthrough = line_index in signature_indexes or line_is_passthrough(line)
        if passthrough:
            flush_translation()
            pending_passthrough_lines.append(line)
        else:
            flush_passthrough()
            pending_translation_lines.append(line)
    flush_translation()
    flush_passthrough()
    return units


def build_translation_prompt(context: str, source_chunk: str) -> str:
    return (
        "Use the following reference material as context.\n"
        "--- CONTEXT START ---\n"
        f"{context}\n"
        "--- CONTEXT END ---\n\n"
        "Now translate the document below from Spanish to English.\n"
        "Do not translate names, dates, codes, identification numbers, or "
        "machine-readable passport lines.\n"
        "Tokens formatted as __PRESERVE_LETTERS__ are protected values. Copy "
        "each one exactly once and do not alter it.\n"
        "Preserve the source line breaks as closely as possible.\n"
        "Output only the translation, no comments, document analysis, summary, "
        "or Markdown. Return only raw translated text.\n\n"
        "--- DOCUMENT START ---\n"
        f"{source_chunk}\n"
        "--- DOCUMENT END ---\n"
    )


def clean_ollama_output(output: str) -> str:
    cleaned = str(output or "").strip()
    if cleaned.startswith("```") and cleaned.endswith("```"):
        cleaned = re.sub(r"^```(?:text)?\s*", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s*```$", "", cleaned)
    cleaned = OUTPUT_MARKER_PATTERN.sub("", cleaned)
    return normalize_document_text(cleaned)


def run_ollama_translation(
    model: str,
    prompt: str,
    *,
    timeout_seconds: int,
) -> str:
    ollama_path = shutil.which("ollama")
    if not ollama_path:
        raise DocumentTranslationError(
            "Ollama is not installed or is not available on PATH."
        )
    try:
        completed = subprocess.run(
            [ollama_path, "run", model],
            input=prompt,
            text=True,
            capture_output=True,
            check=False,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        raise DocumentTranslationError(
            f"Ollama timed out after {timeout_seconds} seconds."
        ) from exc
    except OSError as exc:
        raise DocumentTranslationError(f"Unable to execute Ollama: {exc}") from exc

    if completed.returncode != 0:
        error = normalize_document_text(completed.stderr)[:1_000]
        raise DocumentTranslationError(
            f"Ollama exited with code {completed.returncode}: {error or '<no stderr>'}"
        )
    translated_text = clean_ollama_output(completed.stdout)
    if not translated_text:
        raise DocumentTranslationError("Ollama returned an empty translation.")
    return translated_text


def restore_and_validate_protected_values(
    translated_text: str,
    values_by_placeholder: dict[str, str],
) -> str:
    restored = translated_text
    invalid_placeholders = [
        placeholder
        for placeholder in values_by_placeholder
        if restored.count(placeholder) != 1
    ]
    if invalid_placeholders:
        preview = ", ".join(invalid_placeholders[:5])
        raise DocumentTranslationError(
            "The model omitted or duplicated protected values: " + preview
        )
    for placeholder, original_value in values_by_placeholder.items():
        restored = restored.replace(placeholder, original_value)
    return normalize_document_text(restored)


def translate_document(
    document: SourceDocument,
    *,
    context: str,
    model: str,
    maximum_source_characters: int,
    timeout_seconds: int,
) -> str:
    units = build_translation_units(
        document.source_text,
        parse_exact_context_translations(context),
    )
    model_chunks = [
        chunk
        for unit in units
        if unit.translate
        for chunk in split_source_text(unit.text, maximum_source_characters)
    ]
    translated_parts: list[str] = []
    translated_chunk_number = 0
    for unit in units:
        if not unit.translate:
            translated_parts.append(unit.text)
            continue
        translated_unit_chunks = []
        for chunk in split_source_text(unit.text, maximum_source_characters):
            translated_chunk_number += 1
            protected = protect_nontranslatable_values(chunk)
            print(
                f"translating_chunk: document={document.document_id} "
                f"chunk={translated_chunk_number}/{len(model_chunks)} "
                f"chars={len(protected.text)}",
                flush=True,
            )
            model_output = run_ollama_translation(
                model,
                build_translation_prompt(context, protected.text),
                timeout_seconds=timeout_seconds,
            )
            translated_unit_chunks.append(
                restore_and_validate_protected_values(
                    model_output,
                    protected.values_by_placeholder,
                )
            )
        translated_parts.append("\n".join(translated_unit_chunks))
    return normalize_document_text("\n".join(translated_parts))


def source_preview(value: str, maximum: int = 300) -> str:
    return normalize_document_text(value).replace("\n", " ")[:maximum]


def load_documents(args: argparse.Namespace) -> list[SourceDocument]:
    documents = [load_ocr_json_document(path) for path in args.ocr_json]
    documents.extend(
        load_database_document(
            document_id,
            ocr_provider=args.ocr_provider,
            reuse_existing_ocr=not args.no_reuse_existing_ocr,
        )
        for document_id in args.document_id
    )
    return documents


def export_rows(output_path: Path, rows: Iterable[dict[str, object]]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8-sig", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    args = parse_arguments()
    if not args.ocr_json and not args.document_id:
        raise ValueError("Provide at least one --ocr-json or --document-id.")
    if args.max_source_characters < 500:
        raise ValueError("--max-source-characters must be at least 500.")
    if args.ollama_timeout_seconds < 1:
        raise ValueError("--ollama-timeout-seconds must be positive.")
    if not args.context.is_file():
        raise ValueError(f"Context file does not exist: {args.context}")
    context = normalize_document_text(args.context.read_text(encoding="utf-8"))

    documents = load_documents(args)
    print("\n=== input summary ===")
    print(f"documents: {len(documents)}")
    print(f"model: {args.model}")
    print(f"context: {args.context}")
    print("document_samples:")
    for document in documents[:5]:
        print(
            {
                "document_id": document.document_id,
                "hash": document.document_hash,
                "provider": document.ocr_provider,
                "pages": document.page_count,
                "characters": len(document.source_text),
                "preview": source_preview(document.source_text),
            }
        )

    rows = []
    completed_count = 0
    failed_count = 0
    for document in documents:
        try:
            translated_text = translate_document(
                document,
                context=context,
                model=args.model,
                maximum_source_characters=args.max_source_characters,
                timeout_seconds=args.ollama_timeout_seconds,
            )
            status = "completed"
            error = ""
            completed_count += 1
        except Exception as exc:
            translated_text = ""
            status = "failed"
            error = str(exc)
            failed_count += 1
            print(
                f"translation_failed: document={document.document_id} error={error}",
                flush=True,
            )
        rows.append(
            {
                "document_id": document.document_id,
                "document_hash": document.document_hash,
                "source_file": document.source_file,
                "ocr_provider": document.ocr_provider,
                "page_count": document.page_count,
                "source_characters": len(document.source_text),
                "translated_characters": len(translated_text),
                "status": status,
                "translated_text": translated_text,
                "error": error,
            }
        )

    export_rows(args.output, rows)
    print("\n=== translation summary ===")
    print(f"documents: {len(documents)}")
    print(f"completed: {completed_count}")
    print(f"failed: {failed_count}")
    print(f"output_csv: {args.output}")
    if failed_count:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
