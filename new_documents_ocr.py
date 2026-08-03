"""OCR contact documents missing text_extraction rows into CSV and the database.

The script retries prior CSV failures, chunks oversized PDFs through the shared
OCR provider, writes successful text extraction results into
``document_processing``, and still appends every attempt to the resumable CSV.
"""

from __future__ import annotations

import base64
import binascii
import csv
import json
import sys
from io import BytesIO
from pathlib import Path
from time import perf_counter

from dotenv import load_dotenv
import pypdfium2
from sqlalchemy import MetaData, Table, select
from sqlalchemy.sql import and_


REPO_ROOT = Path(__file__).resolve().parent
load_dotenv(REPO_ROOT / ".env", override=False)

from src.components.clients.document_processing import (
    DocumentOCRResult,
    GOOGLE_DOCUMENT_AI_PROVIDER,
    OCR_MAX_RENDER_DIMENSION,
    OCR_RENDER_DPI,
    assess_ocr_text,
    extract_document_ocr,
    upsert_document_text_extraction_record,
    validate_ocr_provider_configuration,
)
from src.utils.connectors.supabase import db, initialize_database

initialize_database()


PROCESS_TYPE = "text_extraction"
TARGET_COUNT = 100
GOOGLE_DOCUMENT_AI_PAGE_LIMIT = 15
OUTPUT_CSV_PATH = REPO_ROOT / "new_documents_ocr_extractions.csv"
PREVIEW_LENGTH = 300

TERMINAL_FAILURE_PREFIXES = (
    "Unsupported OCR mime type:",
    "Failed to load document (PDFium: Incorrect password error)",
    "Failed to load document (PDFium: Data format error)",
    "Failed to open image for OCR:",
    "document data is empty",
    "document data is not valid base64",
    "document not found:",
    "contact_document has no document_id",
)

CSV_FIELDS = [
    "contact_document_id",
    "document_id",
    "document_language",
    "type",
    "category",
    "mime_type",
    "provider",
    "model_version",
    "pipeline_version",
    "status",
    "elapsed_seconds",
    "page_count",
    "region_count",
    "line_count",
    "word_count",
    "output_chars",
    "ocr_quality_score",
    "ocr_quality_status",
    "average_confidence",
    "provider_image_quality_score",
    "quality_reasons",
    "quality_defects",
    "output_preview",
    "output_text",
    "error",
]


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
                "Move or rename it before running a new extraction."
            )
        return list(reader)


def append_row(row: dict) -> None:
    write_header = not OUTPUT_CSV_PATH.exists() or OUTPUT_CSV_PATH.stat().st_size == 0
    with OUTPUT_CSV_PATH.open("a", newline="", encoding="utf-8") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=CSV_FIELDS)
        if write_header:
            writer.writeheader()
        writer.writerow(row)


def failure_is_terminal(row: dict) -> bool:
    if str(row.get("status") or "").strip().lower() != "failed":
        return False
    error = str(row.get("error") or "").strip()
    return any(error.startswith(prefix) for prefix in TERMINAL_FAILURE_PREFIXES)


def read_unprocessed_contact_documents(
    existing_rows: list[dict],
) -> tuple[list[dict], dict[str, int]]:
    completed_contact_document_ids = {
        str(row.get("contact_document_id") or "").strip()
        for row in existing_rows
        if str(row.get("contact_document_id") or "").strip()
        and str(row.get("status") or "").strip().lower() == "completed"
    }
    completed_document_ids_from_csv = {
        str(row.get("document_id") or "").strip()
        for row in existing_rows
        if str(row.get("document_id") or "").strip()
        and str(row.get("status") or "").strip().lower() == "completed"
    }
    terminal_contact_document_ids = {
        str(row.get("contact_document_id") or "").strip()
        for row in existing_rows
        if str(row.get("contact_document_id") or "").strip()
        and failure_is_terminal(row)
    }
    terminal_document_ids = {
        str(row.get("document_id") or "").strip()
        for row in existing_rows
        if str(row.get("document_id") or "").strip()
        and failure_is_terminal(row)
    }
    terminal_missing_document_contact_ids = {
        str(row.get("contact_document_id") or "").strip()
        for row in existing_rows
        if str(row.get("contact_document_id") or "").strip()
        and not str(row.get("document_id") or "").strip()
        and failure_is_terminal(row)
    }

    metadata = MetaData()
    contact_document = Table(
        "contact_document",
        metadata,
        autoload_with=db.engine,
    )
    document_processing = Table(
        "document_processing",
        metadata,
        autoload_with=db.engine,
    )

    processed_documents_subquery = (
        select(document_processing.c.document_id)
        .where(
            and_(
                document_processing.c.process_type == PROCESS_TYPE,
                document_processing.c.status == "completed",
                document_processing.c.output_text.isnot(None),
                document_processing.c.output_text != "",
            )
        )
        .distinct()
        .subquery()
    )

    @db.with_session(commit=False)
    def _read(session):
        query = (
            select(
                contact_document,
                processed_documents_subquery.c.document_id.label(
                    "processed_document_id"
                ),
            )
            .outerjoin(
                processed_documents_subquery,
                contact_document.c.document_id
                == processed_documents_subquery.c.document_id,
            )
            .order_by(contact_document.c.created.desc(), contact_document.c.id.desc())
        )

        selected = []
        selected_document_ids = set()
        eligible_contact_document_rows = 0
        eligible_unique_documents = set()
        already_processed_unique_documents = set()
        missing_document_id_rows = 0

        for row in session.execute(query).mappings().yield_per(500):
            contact_document_id = str(row.get("id") or "").strip()
            document_id = str(row.get("document_id") or "").strip()
            has_processing = row.get("processed_document_id") is not None

            if contact_document_id in terminal_contact_document_ids:
                continue
            if document_id and document_id in terminal_document_ids:
                continue

            if not document_id:
                missing_document_id_rows += 1
                if contact_document_id not in completed_contact_document_ids:
                    selected.append(row)
                    if len(selected) == TARGET_COUNT:
                        break
                continue

            if has_processing:
                already_processed_unique_documents.add(document_id)
                continue

            eligible_contact_document_rows += 1
            eligible_unique_documents.add(document_id)

            if contact_document_id in completed_contact_document_ids:
                continue
            if document_id in completed_document_ids_from_csv:
                continue
            if document_id in selected_document_ids:
                continue

            selected.append(row)
            selected_document_ids.add(document_id)
            if len(selected) == TARGET_COUNT:
                break

        return selected, {
            "eligible_contact_document_rows": eligible_contact_document_rows,
            "eligible_unique_documents": len(eligible_unique_documents),
            "already_processed_unique_documents": len(
                already_processed_unique_documents
            ),
            "missing_document_id_rows": missing_document_id_rows,
            "terminal_failures_skipped": len(terminal_document_ids)
            + len(terminal_missing_document_contact_ids),
        }

    return _read()


def read_document(document_id: str) -> dict | None:
    rows = db.read(table="document", query={"id": document_id}) or []
    return rows[0] if rows else None


def get_pdf_page_count(file_bytes: bytes) -> int:
    pdf = pypdfium2.PdfDocument(BytesIO(file_bytes))
    try:
        return len(pdf)
    finally:
        pdf.close()


def iter_page_chunks(page_count: int, chunk_size: int) -> list[list[int]]:
    return [
        list(range(start, min(start + chunk_size, page_count + 1)))
        for start in range(1, page_count + 1, chunk_size)
    ]


def aggregate_ocr_results(results: list[DocumentOCRResult]) -> DocumentOCRResult:
    if not results:
        raise ValueError("At least one OCR result is required to aggregate.")

    pages = []
    combined_quality_defects = []
    weighted_provider_quality_total = 0.0
    weighted_provider_quality_pages = 0
    total_low_confidence_regions = 0
    total_empty_pages = 0
    total_region_count = 0
    total_line_count = 0
    total_word_count = 0

    for result in results:
        pages.extend(result.pages)
        quality = result.quality
        if quality:
            total_low_confidence_regions += quality.low_confidence_region_count
            total_empty_pages += quality.empty_page_count
            total_region_count += quality.region_count
            if quality.provider_quality_score is not None and quality.page_count > 0:
                weighted_provider_quality_total += (
                    quality.provider_quality_score * quality.page_count
                )
                weighted_provider_quality_pages += quality.page_count
            combined_quality_defects.extend(quality.quality_defects or [])

        total_line_count += sum(len(page.lines) for page in result.pages)
        total_word_count += sum(len(page.words) for page in result.pages)

    aggregate = DocumentOCRResult(
        pages=pages,
        provider=results[0].provider,
        model_version=results[0].model_version,
        pipeline_version=results[0].pipeline_version,
        device=results[0].device,
        languages=results[0].languages,
        source_hash=results[0].source_hash,
    )
    quality = assess_ocr_text(
        aggregate.text,
        page_count=len(pages),
        region_count=total_region_count,
        empty_page_count=total_empty_pages,
        low_confidence_region_count=total_low_confidence_regions,
    )
    quality.provider_quality_score = (
        weighted_provider_quality_total / weighted_provider_quality_pages
        if weighted_provider_quality_pages
        else None
    )
    quality.quality_defects = combined_quality_defects
    aggregate.quality = quality
    return aggregate


def extract_document_ocr_with_chunking(
    file_bytes: bytes,
    source_language: str | None,
    mime_type: str,
):
    normalized_mime_type = str(mime_type or "").strip().lower()
    if normalized_mime_type != "application/pdf":
        return extract_document_ocr(
            file_bytes,
            source_language=source_language,
            mime_type=normalized_mime_type,
            provider=GOOGLE_DOCUMENT_AI_PROVIDER,
            render_dpi=OCR_RENDER_DPI,
            max_render_dimension=OCR_MAX_RENDER_DIMENSION,
            use_cache=True,
        )

    page_count = get_pdf_page_count(file_bytes)
    if page_count <= GOOGLE_DOCUMENT_AI_PAGE_LIMIT:
        return extract_document_ocr(
            file_bytes,
            source_language=source_language,
            mime_type=normalized_mime_type,
            provider=GOOGLE_DOCUMENT_AI_PROVIDER,
            render_dpi=OCR_RENDER_DPI,
            max_render_dimension=OCR_MAX_RENDER_DIMENSION,
            use_cache=True,
        )

    chunk_results = []
    for chunk_page_numbers in iter_page_chunks(page_count, GOOGLE_DOCUMENT_AI_PAGE_LIMIT):
        chunk_results.append(
            extract_document_ocr(
                file_bytes,
                source_language=source_language,
                mime_type=normalized_mime_type,
                provider=GOOGLE_DOCUMENT_AI_PROVIDER,
                render_dpi=OCR_RENDER_DPI,
                max_render_dimension=OCR_MAX_RENDER_DIMENSION,
                use_cache=True,
                page_numbers=chunk_page_numbers,
            )
        )

    return aggregate_ocr_results(chunk_results)


def decode_document(document: dict) -> bytes:
    encoded_data = str(document.get("data") or "").strip()
    if not encoded_data:
        raise ValueError("document data is empty")
    try:
        return base64.b64decode(encoded_data, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ValueError("document data is not valid base64") from exc


def base_row(contact_document: dict, mime_type: str = "") -> dict:
    return {
        "contact_document_id": str(contact_document.get("id") or "").strip(),
        "document_id": str(contact_document.get("document_id") or "").strip(),
        "document_language": contact_document.get("document_language"),
        "type": contact_document.get("type"),
        "category": contact_document.get("category"),
        "mime_type": mime_type,
    }


def failed_row(
    contact_document: dict,
    error: str,
    mime_type: str = "",
    elapsed_seconds: float = 0.0,
) -> dict:
    return {
        **base_row(contact_document, mime_type),
        "provider": GOOGLE_DOCUMENT_AI_PROVIDER,
        "model_version": None,
        "pipeline_version": None,
        "status": "failed",
        "elapsed_seconds": round(elapsed_seconds, 3),
        "page_count": 0,
        "region_count": 0,
        "line_count": 0,
        "word_count": 0,
        "output_chars": 0,
        "ocr_quality_score": 0,
        "ocr_quality_status": "low",
        "average_confidence": None,
        "provider_image_quality_score": None,
        "quality_reasons": "ocr_failed",
        "quality_defects": "[]",
        "output_preview": "<empty>",
        "output_text": "",
        "error": error,
    }


def completed_row(
    contact_document: dict,
    mime_type: str,
    result: DocumentOCRResult,
    elapsed_seconds: float,
) -> dict:
    output_text = result.text.strip()
    quality = result.quality or assess_ocr_text(output_text)
    quality_data = quality.as_dict()
    return {
        **base_row(contact_document, mime_type),
        "provider": result.provider,
        "model_version": result.model_version,
        "pipeline_version": result.pipeline_version,
        "status": "completed",
        "elapsed_seconds": round(elapsed_seconds, 3),
        "page_count": len(result.pages),
        "region_count": sum(len(page.regions) for page in result.pages),
        "line_count": sum(len(page.lines) for page in result.pages),
        "word_count": sum(len(page.words) for page in result.pages),
        "output_chars": len(output_text),
        "ocr_quality_score": quality_data.get("score"),
        "ocr_quality_status": quality_data.get("status"),
        "average_confidence": quality_data.get("average_confidence"),
        "provider_image_quality_score": quality_data.get("provider_quality_score"),
        "quality_reasons": " | ".join(quality_data.get("reasons") or []),
        "quality_defects": json.dumps(
            quality_data.get("quality_defects") or [],
            ensure_ascii=False,
        ),
        "output_preview": output_text[:PREVIEW_LENGTH] or "<empty>",
        "output_text": output_text,
        "error": None,
    }


def write_completed_processing_row(
    contact_document: dict,
    result: DocumentOCRResult,
) -> None:
    document_id = str(contact_document.get("document_id") or "").strip()
    if not document_id:
        raise ValueError("contact_document has no document_id")
    output_text = result.text.strip()
    if not output_text:
        raise ValueError("OCR completed but produced no text")

    upsert_document_text_extraction_record(
        document_id=document_id,
        source_language=contact_document.get("document_language"),
        status="completed",
        output_text=output_text,
        provider=result.provider,
        error=None,
    )


def main() -> None:
    existing_rows = load_existing_rows()
    contact_documents, population = read_unprocessed_contact_documents(existing_rows)
    print(population)
    if not contact_documents:
        print("No contact documents remain without document_processing text extraction.")
        print({"existing_rows": len(existing_rows), "database_writes": 0})
        return

    validate_ocr_provider_configuration(GOOGLE_DOCUMENT_AI_PROVIDER)
    print(
        {
            "process_type": PROCESS_TYPE,
            **population,
            "previously_processed_in_csv": len(existing_rows),
            "batch_requested": TARGET_COUNT,
            "selected": len(contact_documents),
            "output_csv": OUTPUT_CSV_PATH.name,
            "database_writes": 0,
        }
    )

    completed = 0
    failed = 0
    database_writes = 0
    for index, contact_document in enumerate(contact_documents, start=1):
        document_id = str(contact_document.get("document_id") or "").strip()
        category = str(contact_document.get("category") or "<blank>")
        print(
            f"[{index}/{len(contact_documents)}] "
            f"document_id={document_id or '<missing>'} category={category}"
        )
        if not document_id:
            row = failed_row(
                contact_document,
                "contact_document has no document_id",
            )
            append_row(row)
            failed += 1
            print(f"status=failed error={row['error']}")
            continue

        document = read_document(document_id)
        if document is None:
            row = failed_row(
                contact_document,
                f"document not found: {document_id}",
            )
            append_row(row)
            failed += 1
            print(f"status=failed error={row['error']}")
            continue

        mime_type = str(document.get("mime_type") or "").strip().lower()
        try:
            file_bytes = decode_document(document)
        except ValueError as exc:
            row = failed_row(contact_document, str(exc), mime_type)
            append_row(row)
            failed += 1
            print(f"status=failed error={row['error']}")
            continue

        started = perf_counter()
        try:
            result = extract_document_ocr_with_chunking(
                file_bytes,
                mime_type=mime_type,
                source_language=contact_document.get("document_language"),
            )
            write_completed_processing_row(contact_document, result)
            row = completed_row(
                contact_document,
                mime_type,
                result,
                perf_counter() - started,
            )
            completed += 1
            database_writes += 1
            print(
                f"status=completed seconds={row['elapsed_seconds']:.3f} "
                f"quality={row['ocr_quality_status']} chars={row['output_chars']} "
                f"db_write=completed"
            )
        except Exception as exc:
            row = failed_row(
                contact_document,
                str(exc),
                mime_type,
                perf_counter() - started,
            )
            failed += 1
            print(
                f"status=failed seconds={row['elapsed_seconds']:.3f} "
                f"error={row['error']}"
            )
        append_row(row)

    print(
        {
            "batch_requested": TARGET_COUNT,
            "selected": len(contact_documents),
            "completed": completed,
            "failed": failed,
            "cumulative_rows": len(existing_rows) + len(contact_documents),
            "output_csv": OUTPUT_CSV_PATH.name,
            "database_writes": database_writes,
        }
    )


if __name__ == "__main__":
    main()
