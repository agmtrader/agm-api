"""Translate the next 10 database OCR documents to a resumable CSV.

The source text comes only from completed ``document_processing``
``text_extraction`` rows. This script reads the database but never writes to it.
Only clean completed CSV translations are skipped on later runs. Failed and
corrupted completed translations remain eligible for retry.
"""

from __future__ import annotations

import csv
import json
import os
import re
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from dotenv import load_dotenv


REPO_ROOT = Path(__file__).resolve().parent
load_dotenv(REPO_ROOT / ".env", override=False)

from src.utils.connectors.supabase import db, initialize_database

initialize_database()


BATCH_SIZE = 10
MODEL = "translategemma:4b"
PROCESS_TYPE = "text_extraction"
OUTPUT_CSV_PATH = REPO_ROOT / "new_documents_translations.csv"
MAX_SOURCE_CHARACTERS = 2_200
OLLAMA_TIMEOUT_SECONDS = 600
OLLAMA_BASE_URL = "http://127.0.0.1:11434"
TRANSLATION_MEMORY_MAX_CHARACTERS = 8_000
TRANSLATION_MEMORY_MAX_ENTRIES = 100
TRANSLATION_MEMORY_LINE_LIMIT = 120

CSV_FIELDS = [
    "document_id",
    "document_processing_id",
    "document_hash",
    "source_file",
    "mime_type",
    "source_language",
    "ocr_provider",
    "source_characters",
    "translated_characters",
    "elapsed_seconds",
    "status",
    "translated_text",
    "error",
]

SPANISH_MONTHS = (
    "enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|"
    "setiembre|octubre|noviembre|diciembre"
)
DATE_PATTERN = re.compile(
    rf"\b(?:\d{{1,2}}\s+DE\s+)?(?:{SPANISH_MONTHS})"
    rf"(?:\s+DE(?:L)?\s+|\s+)\d{{4}}\b",
    re.IGNORECASE,
)
MRZ_LINE_PATTERN = re.compile(r"(?m)^[A-Z0-9<]{30,}$")
PAGE_COUNTER_PATTERN = re.compile(r"Page\s+\d+\s+of\s+\d+", re.IGNORECASE)
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
    r"(?=[A-Z0-9._/-]*\d)[A-Z0-9]+(?:[._/-][A-Z0-9]+)+\b"
)
CONTROL_CHARACTER_PATTERN = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
TRANSLATION_LEAK_MARKERS = (
    "SPANISH SOURCE:",
    "ENGLISH TRANSLATION:",
    "--- TRANSLATION EXAMPLE ---",
    "--- GLOSSARY START ---",
    "--- GLOSSARY END ---",
    "--- DOCUMENT START ---",
    "--- DOCUMENT END ---",
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


def normalize_document_text(value: str) -> str:
    normalized = str(value or "").replace("\r\n", "\n").replace("\r", "\n")
    return "\n".join(line.rstrip() for line in normalized.splitlines()).strip()


def source_preview(value: str, maximum: int = 300) -> str:
    return normalize_document_text(value).replace("\n", " ")[:maximum]


def translation_output_error(translated_text: str) -> str | None:
    text = str(translated_text or "")
    if not normalize_document_text(text):
        return "Translation output is empty."
    if CONTROL_CHARACTER_PATTERN.search(text) or "\x1b" in text:
        return "Translation output contains terminal control characters."
    if (
        "PRESERVETOKEN" in text
        or "ENDTOKEN" in text
        or "__PRESERVE_" in text
        or re.search(r"ZZX9\d{4}8XZZ", text)
    ):
        return "Translation output contains an unresolved protected-value token."
    leaked_marker = next(
        (marker for marker in TRANSLATION_LEAK_MARKERS if marker in text),
        None,
    )
    if leaked_marker:
        return f"Translation output copied context marker: {leaked_marker}"
    return None


def validate_translation_output(
    translated_text: str,
    *,
    allow_protected_tokens: bool = False,
) -> str:
    error = translation_output_error(translated_text)
    if allow_protected_tokens and error == (
        "Translation output contains an unresolved protected-value token."
    ):
        error = None
    if error:
        raise DocumentTranslationError(error)
    return normalize_document_text(translated_text)


def get_ollama_base_url() -> str:
    value = str(os.getenv("OLLAMA_HOST") or OLLAMA_BASE_URL).strip()
    if not value.startswith(("http://", "https://")):
        value = "http://" + value
    return value.rstrip("/")


def request_ollama_json(
    path: str,
    *,
    timeout_seconds: int,
    payload: dict | None = None,
) -> dict:
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    headers = {"Accept": "application/json"}
    if data is not None:
        headers["Content-Type"] = "application/json"
    request = Request(
        f"{get_ollama_base_url()}{path}",
        data=data,
        headers=headers,
        method="POST" if data is not None else "GET",
    )
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        try:
            detail = str(json.loads(exc.read().decode("utf-8")).get("error"))
        except (OSError, TypeError, ValueError):
            detail = str(exc)
        raise DocumentTranslationError(
            f"Ollama API returned HTTP {exc.code}: {detail[:1_000]}"
        ) from exc
    except (URLError, TimeoutError, OSError) as exc:
        raise DocumentTranslationError(
            f"Unable to connect to Ollama at {get_ollama_base_url()}: {exc}"
        ) from exc
    except (TypeError, ValueError) as exc:
        raise DocumentTranslationError(f"Ollama returned invalid JSON: {exc}") from exc


def validate_ollama_model(model: str) -> None:
    payload = request_ollama_json("/api/tags", timeout_seconds=10)
    installed = {
        str(item.get("name") or item.get("model") or "").strip()
        for item in payload.get("models") or []
        if isinstance(item, dict)
    }
    if model not in installed:
        preview = ", ".join(sorted(installed)[:10]) or "<none>"
        raise DocumentTranslationError(
            f"Required Ollama model is not installed: {model}. Installed: {preview}"
        )


def run_ollama_translation(prompt: str) -> str:
    response = request_ollama_json(
        "/api/generate",
        timeout_seconds=OLLAMA_TIMEOUT_SECONDS,
        payload={
            "model": MODEL,
            "prompt": prompt,
            "stream": False,
            "options": {"temperature": 0},
        },
    )
    return validate_translation_output(
        response.get("response") or "",
        allow_protected_tokens=True,
    )


def protect_nontranslatable_values(source_text: str) -> ProtectedText:
    protected_text = source_text
    values_by_placeholder: dict[str, str] = {}

    def replacement(match: re.Match[str]) -> str:
        placeholder = f"ZZX9{len(values_by_placeholder) + 1:04d}8XZZ"
        values_by_placeholder[placeholder] = match.group(0)
        return placeholder

    for pattern in (
        MRZ_LINE_PATTERN,
        SIGNATURE_LINE_PATTERN,
        LEGAL_ENTITY_LINE_PATTERN,
        TITLED_NAME_PATTERN,
        DATE_PATTERN,
        CODE_PATTERN,
        MACHINE_VALUE_PATTERN,
    ):
        protected_text = pattern.sub(replacement, protected_text)
    return ProtectedText(protected_text, values_by_placeholder)


def restore_protected_values(protected: ProtectedText, translated_text: str) -> str:
    invalid = [
        placeholder
        for placeholder in protected.values_by_placeholder
        if translated_text.count(placeholder) != 1
    ]
    if invalid:
        raise DocumentTranslationError(
            "The model omitted or duplicated protected values: "
            + ", ".join(invalid[:5])
        )
    restored = translated_text
    for placeholder, original in protected.values_by_placeholder.items():
        restored = restored.replace(placeholder, original)
    return validate_translation_output(restored)


def translate_segment_preserving_whitespace(segment: str, glossary: str) -> str:
    if not segment or not re.search(r"[A-Za-zÀ-ÿ]", segment):
        return segment
    leading = segment[: len(segment) - len(segment.lstrip())]
    trailing = segment[len(segment.rstrip()) :]
    core = segment.strip()
    if not core:
        return segment
    translated = run_ollama_translation(build_translation_prompt(glossary, core))
    return leading + translated + trailing


def translate_with_protected_value_fallback(
    protected: ProtectedText,
    glossary: str,
) -> str:
    """Translate around protected tokens and reinsert their values in Python."""
    if not protected.values_by_placeholder:
        return run_ollama_translation(
            build_translation_prompt(glossary, protected.text)
        )
    placeholder_pattern = re.compile(
        "(" + "|".join(map(re.escape, protected.values_by_placeholder)) + ")"
    )
    parts = placeholder_pattern.split(protected.text)
    translated_parts = []
    for part in parts:
        if part in protected.values_by_placeholder:
            translated_parts.append(protected.values_by_placeholder[part])
        else:
            translated_parts.append(
                translate_segment_preserving_whitespace(part, glossary)
            )
    return validate_translation_output("".join(translated_parts))


def split_source_text(source_text: str) -> list[str]:
    chunks: list[str] = []
    current: list[str] = []
    current_length = 0
    for original_line in source_text.splitlines():
        remaining = original_line
        pieces = []
        while len(remaining) > MAX_SOURCE_CHARACTERS:
            split_at = remaining.rfind(" ", 0, MAX_SOURCE_CHARACTERS + 1)
            split_at = split_at if split_at > 0 else MAX_SOURCE_CHARACTERS
            pieces.append(remaining[:split_at].rstrip())
            remaining = remaining[split_at:].lstrip()
        pieces.append(remaining)
        for line in pieces:
            added = len(line) + (1 if current else 0)
            if current and current_length + added > MAX_SOURCE_CHARACTERS:
                chunks.append("\n".join(current).strip())
                current = []
                current_length = 0
            current.append(line)
            current_length += len(line) + (1 if len(current) > 1 else 0)
    if current:
        chunks.append("\n".join(current).strip())
    return [chunk for chunk in chunks if chunk]


def line_is_passthrough(line: str) -> bool:
    stripped = line.strip()
    if not stripped or not re.search(r"[A-Za-zÀ-ÿ]", stripped):
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


def build_translation_units(source_text: str, glossary: str) -> list[TranslationUnit]:
    exact = {}
    for line in glossary.splitlines():
        if "=" in line:
            source, translated = (part.strip() for part in line.split("=", 1))
            if source and translated:
                exact[source.casefold()] = translated
    units: list[TranslationUnit] = []
    pending: list[str] = []

    def flush_pending() -> None:
        if pending:
            units.append(TranslationUnit("\n".join(pending), True))
            pending.clear()

    for line in source_text.splitlines():
        exact_translation = exact.get(line.strip().casefold())
        if exact_translation is not None:
            flush_pending()
            units.append(TranslationUnit(exact_translation, False))
        elif line_is_passthrough(line):
            flush_pending()
            units.append(TranslationUnit(line, False))
        else:
            pending.append(line)
    flush_pending()
    return units


def build_translation_prompt(glossary: str, source_chunk: str) -> str:
    return (
        "Use this Spanish-to-English glossary only as reference. Never output the "
        "glossary itself.\n--- GLOSSARY START ---\n"
        f"{glossary}\n--- GLOSSARY END ---\n\n"
        "Translate the document below from Spanish to English. Do not translate "
        "names, dates, codes, identification numbers, or passport MRZ lines. "
        "Copy every token formatted as ZZX9DIGITS8XZZ exactly once. Preserve line "
        "breaks. Output only raw translated text with no commentary or Markdown.\n"
        "--- DOCUMENT START ---\n"
        f"{source_chunk}\n--- DOCUMENT END ---"
    )


def translate_document(document: SourceDocument, glossary: str) -> str:
    units = build_translation_units(document.source_text, glossary)
    chunk_count = sum(len(split_source_text(unit.text)) for unit in units if unit.translate)
    translated_parts: list[str] = []
    chunk_number = 0
    for unit in units:
        if not unit.translate:
            translated_parts.append(unit.text)
            continue
        unit_parts = []
        for chunk in split_source_text(unit.text):
            chunk_number += 1
            protected = protect_nontranslatable_values(chunk)
            print(
                f"translating_chunk: document={document.document_id} "
                f"chunk={chunk_number}/{chunk_count} chars={len(protected.text)}",
                flush=True,
            )
            try:
                model_output = run_ollama_translation(
                    build_translation_prompt(glossary, protected.text)
                )
                restored = restore_protected_values(protected, model_output)
            except DocumentTranslationError as exc:
                print(
                    f"fallback_chunk: document={document.document_id} "
                    f"chunk={chunk_number}/{chunk_count} "
                    f"reason={exc}",
                    flush=True,
                )
                restored = translate_with_protected_value_fallback(
                    protected,
                    glossary,
                )
            unit_parts.append(restored)
        translated_parts.append("\n".join(unit_parts))
    return validate_translation_output("\n".join(translated_parts))


def configure_csv_field_limit() -> None:
    limit = sys.maxsize
    while True:
        try:
            csv.field_size_limit(limit)
            return
        except OverflowError:
            limit //= 10


configure_csv_field_limit()


def load_existing_rows() -> list[dict]:
    if not OUTPUT_CSV_PATH.exists() or OUTPUT_CSV_PATH.stat().st_size == 0:
        return []
    with OUTPUT_CSV_PATH.open(newline="", encoding="utf-8-sig") as source_file:
        reader = csv.DictReader(source_file)
        if reader.fieldnames != CSV_FIELDS:
            raise RuntimeError(
                f"{OUTPUT_CSV_PATH.name} has an incompatible header. "
                "Move or rename it before running a new translation batch."
            )
        return list(reader)


def append_row(row: dict) -> None:
    write_header = not OUTPUT_CSV_PATH.exists() or OUTPUT_CSV_PATH.stat().st_size == 0
    with OUTPUT_CSV_PATH.open("a", newline="", encoding="utf-8") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=CSV_FIELDS)
        if write_header:
            writer.writeheader()
        writer.writerow(row)


def processing_sort_key(row: dict) -> tuple[str, str, str]:
    return (
        str(row.get("updated") or ""),
        str(row.get("created") or ""),
        str(row.get("id") or ""),
    )


def is_clean_completed_translation(row: dict) -> bool:
    return (
        str(row.get("status") or "").strip().lower() == "completed"
        and translation_output_error(row.get("translated_text") or "") is None
    )


def build_translation_memory(
    existing_rows: list[dict],
    processing_by_document_id: dict[str, dict],
) -> tuple[str, int]:
    """Build a bounded exact-line glossary from clean prior translations."""
    translations_by_source: dict[str, Counter[str]] = defaultdict(Counter)
    source_spelling: dict[str, str] = {}
    for row in existing_rows:
        if not is_clean_completed_translation(row):
            continue
        document_id = str(row.get("document_id") or "").strip()
        processing = processing_by_document_id.get(document_id)
        source_text = normalize_document_text(
            processing.get("output_text") if processing else ""
        )
        translated_text = normalize_document_text(row.get("translated_text") or "")
        if not source_text or not translated_text:
            continue
        source_lines = source_text.splitlines()
        translated_lines = translated_text.splitlines()
        if len(source_lines) != len(translated_lines):
            continue
        for source_line, translated_line in zip(
            source_lines,
            translated_lines,
            strict=True,
        ):
            source_line = source_line.strip()
            translated_line = translated_line.strip()
            if (
                not source_line
                or not translated_line
                or source_line.casefold() == translated_line.casefold()
                or len(source_line) > TRANSLATION_MEMORY_LINE_LIMIT
                or len(translated_line) > TRANSLATION_MEMORY_LINE_LIMIT
                or "=" in source_line
                or "=" in translated_line
                or re.search(r"\d", source_line)
                or not re.search(r"[A-Za-zÀ-ÿ]", source_line)
            ):
                continue
            source_key = source_line.casefold()
            source_spelling.setdefault(source_key, source_line)
            translations_by_source[source_key][translated_line] += 1

    glossary_lines: list[str] = []
    total_characters = 0
    ranked_sources = sorted(
        translations_by_source,
        key=lambda key: (
            -sum(translations_by_source[key].values()),
            source_spelling[key].casefold(),
        ),
    )
    for source_key in ranked_sources:
        translated_line, _count = translations_by_source[source_key].most_common(1)[0]
        glossary_line = f"{source_spelling[source_key]} = {translated_line}"
        if total_characters + len(glossary_line) > TRANSLATION_MEMORY_MAX_CHARACTERS:
            continue
        glossary_lines.append(glossary_line)
        total_characters += len(glossary_line) + 1
        if len(glossary_lines) == TRANSLATION_MEMORY_MAX_ENTRIES:
            break

    return "\n".join(glossary_lines), len(glossary_lines)


def read_translation_batch(
    existing_rows: list[dict],
) -> tuple[list[SourceDocument], dict[str, dict], dict[str, int], str]:
    attempted_document_ids = {
        str(row.get("document_id") or "").strip()
        for row in existing_rows
        if str(row.get("document_id") or "").strip()
    }
    completed_document_ids = {
        str(row.get("document_id") or "").strip()
        for row in existing_rows
        if is_clean_completed_translation(row)
        and str(row.get("document_id") or "").strip()
    }
    invalid_completed_rows = sum(
        str(row.get("status") or "").strip().lower() == "completed"
        and not is_clean_completed_translation(row)
        for row in existing_rows
    )

    # Match new_documents_ocr.py's database sources, but request no document
    # payload because completed OCR text is already in document_processing.
    documents = db.read(table="document", query={}, exclude_columns=["data"]) or []
    processing_rows = db.read(
        table="document_processing",
        query={"process_type": PROCESS_TYPE},
    ) or []

    completed_processing_by_document_id: dict[str, dict] = {}
    for row in processing_rows:
        document_id = str(row.get("document_id") or "").strip()
        output_text = normalize_document_text(row.get("output_text") or "")
        if (
            not document_id
            or str(row.get("status") or "").strip().lower() != "completed"
            or not output_text
        ):
            continue
        current = completed_processing_by_document_id.get(document_id)
        if current is None or processing_sort_key(row) > processing_sort_key(current):
            completed_processing_by_document_id[document_id] = row

    eligible: list[SourceDocument] = []
    metadata_by_document_id: dict[str, dict] = {}
    documents_with_completed_ocr = 0
    for document in documents:
        document_id = str(document.get("id") or "").strip()
        processing = completed_processing_by_document_id.get(document_id)
        if processing is None:
            continue
        documents_with_completed_ocr += 1
        if document_id in completed_document_ids:
            continue

        source_text = normalize_document_text(processing.get("output_text") or "")
        eligible.append(
            SourceDocument(
                document_id=document_id,
                document_hash=str(document.get("sha1_checksum") or "").strip(),
                source_file=str(document.get("file_name") or document_id).strip(),
                ocr_provider=str(processing.get("provider") or "").strip(),
                page_count=max(1, source_text.count("\f") + 1),
                source_text=source_text,
            )
        )
        metadata_by_document_id[document_id] = {
            "document_processing_id": str(processing.get("id") or "").strip(),
            "mime_type": str(document.get("mime_type") or "").strip(),
            "source_language": str(processing.get("source_language") or "").strip(),
        }

    translation_memory, translation_memory_examples = build_translation_memory(
        existing_rows,
        completed_processing_by_document_id,
    )
    return eligible[:BATCH_SIZE], metadata_by_document_id, {
        "database_documents": len(documents),
        "text_extraction_rows": len(processing_rows),
        "documents_with_completed_ocr": documents_with_completed_ocr,
        "previously_attempted_in_csv": len(attempted_document_ids),
        "previously_clean_completed_in_csv": len(completed_document_ids),
        "invalid_completed_rows_to_retry": invalid_completed_rows,
        "translation_memory_examples": translation_memory_examples,
        "translation_memory_characters": len(translation_memory),
    }, translation_memory


def main() -> None:
    existing_rows = load_existing_rows()
    documents, metadata_by_document_id, population, translation_memory = (
        read_translation_batch(existing_rows)
    )

    print(
        {
            **population,
            "batch_requested": BATCH_SIZE,
            "selected": len(documents),
            "model": MODEL,
            "output_csv": OUTPUT_CSV_PATH.name,
            "database_writes": 0,
        }
    )
    if not documents:
        print("No completed OCR documents remain to translate.")
        return

    validate_ollama_model(MODEL)

    print("document_samples:")
    for document in documents[:5]:
        print(
            {
                "document_id": document.document_id,
                "provider": document.ocr_provider,
                "characters": len(document.source_text),
                "preview": source_preview(document.source_text),
            }
        )

    completed = 0
    failed = 0
    for index, document in enumerate(documents, start=1):
        print(f"[{index}/{len(documents)}] document_id={document.document_id}")
        started = perf_counter()
        try:
            translated_text = translate_document(document, translation_memory)
            status = "completed"
            error = ""
            completed += 1
        except Exception as exc:
            translated_text = ""
            status = "failed"
            error = str(exc)
            failed += 1
            print(f"status=failed error={error}", flush=True)

        metadata = metadata_by_document_id[document.document_id]
        append_row(
            {
                "document_id": document.document_id,
                "document_processing_id": metadata["document_processing_id"],
                "document_hash": document.document_hash,
                "source_file": document.source_file,
                "mime_type": metadata["mime_type"],
                "source_language": metadata["source_language"],
                "ocr_provider": document.ocr_provider,
                "source_characters": len(document.source_text),
                "translated_characters": len(translated_text),
                "elapsed_seconds": round(perf_counter() - started, 3),
                "status": status,
                "translated_text": translated_text,
                "error": error,
            }
        )

    print(
        {
            "batch_requested": BATCH_SIZE,
            "selected": len(documents),
            "completed": completed,
            "failed": failed,
            "cumulative_rows": len(existing_rows) + len(documents),
            "output_csv": OUTPUT_CSV_PATH.name,
            "database_writes": 0,
        }
    )
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
